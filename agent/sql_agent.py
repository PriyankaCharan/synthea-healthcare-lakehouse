"""Text-to-SQL agent over the Gold layer.

Flow for one question:
  1. prompt = business rules + Gold schema (from table/column comments) + worked examples
  2. a Databricks-hosted chat model writes one SQL query
  3. guardrails: read-only, Gold tables only, row limit
  4. the query runs (Spark in a notebook, SQL warehouse in the app)
  5. on error, the error goes back to the model for a corrected query (up to max_attempts)
  6. the model writes a short plain-English answer from the result

The same class is used by the notebook, the evaluation and the chat app; only
the `llm` and `run_sql` callables differ.
"""
import re
import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional

import pandas as pd

from .guardrails import GuardrailError, check_sql
from .prompts import ANSWER_TEMPLATE, FEW_SHOT, FIX_TEMPLATE, SYSTEM_TEMPLATE

LLM = Callable[[List[dict]], str]          # messages -> text
RunSQL = Callable[[str], pd.DataFrame]      # sql -> result


# ---------------------------------------------------------------------------
# Model access
# ---------------------------------------------------------------------------
def _message_text(content) -> str:
    """Databricks chat endpoints return either a string or, for reasoning
    models, a list of parts such as {"type": "reasoning"} and {"type": "text"}."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    parts = []
    for p in content:
        p = p if isinstance(p, dict) else getattr(p, "__dict__", {})
        if p.get("type") == "text":
            parts.append(p.get("text", ""))
    return "".join(parts)


def databricks_llm(endpoint: str, temperature: float = 0.0, max_tokens: int = 2000) -> LLM:
    """Chat function backed by a Databricks Model Serving endpoint.
    Authentication comes from the notebook / app context automatically."""
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient().serving_endpoints.get_open_ai_client()

    def call(messages: List[dict]) -> str:
        resp = client.chat.completions.create(
            model=endpoint, messages=messages, temperature=temperature, max_tokens=max_tokens)
        return _message_text(resp.choices[0].message.content)

    return call


def find_endpoint(preferred: str = "databricks-qwen35-122b-a10b") -> str:
    """Return the full name of a ready serving endpoint whose name contains `preferred`."""
    from databricks.sdk import WorkspaceClient

    names = [e.name for e in WorkspaceClient().serving_endpoints.list()]
    key = preferred.lower().replace(".", "-")
    matches = [n for n in names if key in n.lower().replace(".", "-")]
    if not matches:
        raise ValueError(f"No serving endpoint matches '{preferred}'. Available: {sorted(names)}")
    return sorted(matches, key=len)[0]


# ---------------------------------------------------------------------------
# Schema context
# ---------------------------------------------------------------------------
def build_schema_context(run_sql: RunSQL, catalog: str = "workspace", schema: str = "gold") -> str:
    """Describe every Gold table and column using the comments written by 03_gold_model."""
    tables = run_sql(f"""
        SELECT table_name, comment FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}' ORDER BY table_name""")
    cols = run_sql(f"""
        SELECT table_name, column_name, full_data_type, comment
        FROM {catalog}.information_schema.columns
        WHERE table_schema = '{schema}' ORDER BY table_name, ordinal_position""")
    lines = []
    for _, t in tables.iterrows():
        lines.append(f"\n### {catalog}.{schema}.{t['table_name']}")
        if pd.notna(t["comment"]) and t["comment"]:
            lines.append(str(t["comment"]))
        for _, c in cols[cols["table_name"] == t["table_name"]].iterrows():
            note = f"  -- {c['comment']}" if pd.notna(c["comment"]) and c["comment"] else ""
            lines.append(f"- {c['column_name']} {c['full_data_type']}{note}")
    lines.append("\nJoin keys: patient_id, provider_id, organization_id, payer_id; "
                 "date_key / onset_date_key / start_date_key join dim_date.date_key.")
    return "\n".join(lines).strip()


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------
_SQL_BLOCK = re.compile(r"```(?:sql)?\s*(.*?)```", re.S | re.I)
_THINK = re.compile(r"<think>.*?</think>", re.S | re.I)


def extract_sql(text: str) -> str:
    text = _THINK.sub("", text or "")
    blocks = _SQL_BLOCK.findall(text)
    if blocks:
        return blocks[-1].strip()
    # fall back to the first line that starts a query
    m = re.search(r"\b(WITH|SELECT)\b.*", text, re.S | re.I)
    return m.group(0).strip() if m else text.strip()


@dataclass
class AgentResult:
    question: str
    sql: Optional[str] = None
    data: Optional[pd.DataFrame] = None
    answer: Optional[str] = None
    error: Optional[str] = None
    attempts: int = 0
    seconds: float = 0.0
    trace: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.error is None and self.data is not None


class SQLAgent:
    def __init__(self, llm: LLM, run_sql: RunSQL, schema_context: str, catalog: str = "workspace",
                 max_attempts: int = 3, row_limit: int = 1000, answer_llm: Optional[LLM] = None):
        self.llm = llm
        self.answer_llm = answer_llm or llm
        self.run_sql = run_sql
        self.catalog = catalog
        self.max_attempts = max_attempts
        self.row_limit = row_limit
        self.system = SYSTEM_TEMPLATE.format(schema=schema_context, catalog=catalog)
        self.examples = []
        for ex in FEW_SHOT:
            self.examples += [
                {"role": "user", "content": ex["question"]},
                {"role": "assistant", "content": "```sql\n" + ex["sql"].format(catalog=catalog) + "\n```"},
            ]

    def generate_sql(self, question: str, history: Optional[List[dict]] = None) -> List[dict]:
        return [{"role": "system", "content": self.system}, *self.examples,
                *(history or []), {"role": "user", "content": question}]

    def ask(self, question: str, history: Optional[List[dict]] = None, explain: bool = True) -> AgentResult:
        start = time.time()
        res = AgentResult(question=question)
        messages = self.generate_sql(question, history)
        for attempt in range(1, self.max_attempts + 1):
            res.attempts = attempt
            reply = self.llm(messages)
            sql = extract_sql(reply)
            res.trace.append({"attempt": attempt, "sql": sql})
            try:
                checked = check_sql(sql, catalog=self.catalog, row_limit=self.row_limit)
                res.sql = checked.sql
                res.data = self.run_sql(checked.sql)
                res.error = None
                break
            except GuardrailError as e:
                res.error = f"Blocked by guardrail: {e}"
                res.sql = sql
            except Exception as e:  # SQL error from the engine
                res.error = str(e).split("\n")[0][:500]
                res.sql = sql
            res.trace[-1]["error"] = res.error
            messages = messages + [
                {"role": "assistant", "content": reply},
                {"role": "user", "content": FIX_TEMPLATE.format(sql=sql, error=res.error)},
            ]
        if res.ok and explain:
            res.answer = self.explain(question, res.sql, res.data)
        res.seconds = round(time.time() - start, 2)
        return res

    def explain(self, question: str, sql: str, data: pd.DataFrame, max_rows: int = 20) -> str:
        shown = data.head(max_rows)
        prompt = ANSWER_TEMPLATE.format(question=question, sql=sql, n_rows=len(data),
                                        shown=len(shown), table=shown.to_string(index=False))
        return _THINK.sub("", self.answer_llm([{"role": "user", "content": prompt}])).strip()


def spark_runner(spark) -> RunSQL:
    """Run SQL with the notebook's Spark session."""
    return lambda sql: spark.sql(sql).toPandas()


def warehouse_runner(warehouse_id: str, wait_seconds: int = 50) -> RunSQL:
    """Run SQL on a SQL warehouse through the Statement Execution API (used by the app)."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    w = WorkspaceClient()

    def run(sql: str) -> pd.DataFrame:
        r = w.statement_execution.execute_statement(
            statement=sql, warehouse_id=warehouse_id, wait_timeout=f"{wait_seconds}s")
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(1)
            r = w.statement_execution.get_statement(r.statement_id)
        if r.status.state != StatementState.SUCCEEDED:
            msg = r.status.error.message if r.status.error else r.status.state.value
            raise RuntimeError(msg)
        cols = [c.name for c in r.manifest.schema.columns]
        types = [c.type_name.value if c.type_name else "STRING" for c in r.manifest.schema.columns]
        rows = (r.result.data_array or []) if r.result else []
        df = pd.DataFrame(rows, columns=cols)
        for c, t in zip(cols, types):
            if t in ("INT", "LONG", "SHORT", "BYTE", "BIGINT", "DOUBLE", "FLOAT", "DECIMAL"):
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df

    return run
