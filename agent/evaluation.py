"""Execution-accuracy evaluation for the SQL agent.

For every question in eval/questions.yaml we run the hand-written reference SQL
and the agent's SQL, then compare the *results* (not the SQL text):

  * same number of rows
  * every reference column is matched by a different agent column with the same
    values (order-insensitive; numbers within 1%, and 0.25 vs 25% counts as equal)

Extra columns in the agent's answer are allowed, so a helpful extra column is
not punished, but a missing or wrong number is.
"""
import datetime as dt
import decimal
import math
from dataclasses import asdict, dataclass
from typing import List, Optional

import pandas as pd
import yaml


def load_questions(path: str, catalog: str = "workspace") -> List[dict]:
    with open(path) as f:
        items = yaml.safe_load(f)
    for q in items:
        q["sql"] = q["sql"].replace("{catalog}", catalog)
    return items


def _norm(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, (int, float, decimal.Decimal)):
        return float(v)
    if isinstance(v, (dt.datetime, pd.Timestamp)):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, dt.date):
        return v.isoformat()
    s = str(v).strip()
    try:
        return float(s)
    except ValueError:
        return s.lower()


def _num_close(a: float, b: float) -> bool:
    for x, y in ((a, b), (a, b * 100), (a * 100, b)):
        if abs(x - y) <= max(0.01 * abs(y), 0.011):
            return True
    return False


def _column_values(df: pd.DataFrame, col) -> list:
    return [_norm(v) for v in df[col].tolist()]


def _columns_match(ref: list, cand: list) -> bool:
    if len(ref) != len(cand):
        return False
    ref_num = all(isinstance(v, float) or v is None for v in ref)
    cand_num = all(isinstance(v, float) or v is None for v in cand)
    if ref_num and cand_num:
        r = sorted(v for v in ref if v is not None)
        c = sorted(v for v in cand if v is not None)
        if len(r) != len(c):
            return False
        if all(_num_close(x, y) for x, y in zip(c, r)):
            return True
        # percentage scaling applied to the whole column
        return all(abs(x - y) <= max(0.01 * abs(y), 0.011) for x, y in zip(c, [v * 100 for v in r])) or \
            all(abs(x * 100 - y) <= max(0.01 * abs(y), 0.011) for x, y in zip(c, r))
    return sorted(map(str, ref)) == sorted(map(str, cand))


def results_match(reference: pd.DataFrame, candidate: Optional[pd.DataFrame]) -> bool:
    if candidate is None or len(reference) != len(candidate):
        return False
    used = set()
    for rc in reference.columns:
        ref_vals = _column_values(reference, rc)
        hit = None
        for cc in candidate.columns:
            if cc in used:
                continue
            if _columns_match(ref_vals, _column_values(candidate, cc)):
                hit = cc
                break
        if hit is None:
            return False
        used.add(hit)
    return True


@dataclass
class EvalRow:
    id: str
    category: str
    question: str
    correct: bool
    attempts: int
    seconds: float
    error: Optional[str]
    agent_sql: Optional[str]
    reference_sql: str


def evaluate(agent, questions: List[dict], run_sql) -> pd.DataFrame:
    rows = []
    for q in questions:
        reference = run_sql(q["sql"])
        res = agent.ask(q["question"], explain=False)
        ok = res.ok and results_match(reference, res.data)
        rows.append(EvalRow(id=q["id"], category=q.get("category", ""), question=q["question"],
                            correct=ok, attempts=res.attempts, seconds=res.seconds, error=res.error,
                            agent_sql=res.sql, reference_sql=q["sql"]))
        print(f"{'PASS' if ok else 'FAIL'}  {q['id']}  {q['question']}")
    return pd.DataFrame([asdict(r) for r in rows])
