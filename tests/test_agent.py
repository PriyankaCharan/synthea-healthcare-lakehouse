"""Unit tests for the SQL agent: guardrails, SQL extraction, result matching and
the retry loop. Run with: pytest tests/test_agent.py"""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agent import GuardrailError, SQLAgent, check_sql, extract_sql  # noqa: E402
from agent.evaluation import load_questions, results_match  # noqa: E402

ROOT = os.path.join(os.path.dirname(__file__), "..")


# --- guardrails -------------------------------------------------------------
def test_allows_gold_select_and_adds_limit():
    c = check_sql("SELECT gender, COUNT(*) FROM workspace.gold.dim_patient GROUP BY gender")
    assert "LIMIT 1000" in c.sql
    assert c.tables == ["workspace.gold.dim_patient"]


def test_keeps_existing_limit():
    c = check_sql("SELECT * FROM workspace.gold.dim_patient LIMIT 5")
    assert c.sql.endswith("LIMIT 5")


def test_allows_cte_and_union():
    check_sql("WITH x AS (SELECT patient_id FROM workspace.gold.dim_patient) SELECT COUNT(*) FROM x")
    check_sql("SELECT 1 AS a FROM workspace.gold.dim_payer UNION ALL SELECT 2 FROM workspace.gold.dim_payer")


@pytest.mark.parametrize("sql", [
    "DROP TABLE workspace.gold.dim_patient",
    "DELETE FROM workspace.gold.dim_patient",
    "INSERT INTO workspace.gold.dim_payer SELECT * FROM workspace.gold.dim_payer",
    "UPDATE workspace.gold.dim_patient SET age = 1",
    "SELECT * FROM workspace.bronze.patients",          # PII lives in bronze
    "SELECT * FROM workspace.silver.patient",
    "SELECT * FROM system.access.audit",
    "SELECT 1; DROP TABLE workspace.gold.dim_patient",
    "",
])
def test_blocks_unsafe_sql(sql):
    with pytest.raises(GuardrailError):
        check_sql(sql)


# --- SQL extraction ---------------------------------------------------------
def test_extracts_last_sql_block_and_strips_thinking():
    text = "<think>try ```sql SELECT 0```</think>Here:\n```sql\nSELECT 1 FROM workspace.gold.dim_payer\n```"
    assert extract_sql(text) == "SELECT 1 FROM workspace.gold.dim_payer"


# --- result matching --------------------------------------------------------
def test_results_match_ignores_names_order_and_extra_columns():
    ref = pd.DataFrame({"gender": ["Female", "Male"], "n": [202, 198]})
    cand = pd.DataFrame({"n_patients": [198, 202], "sex": ["male", "female"], "share": [0.49, 0.51]})
    assert results_match(ref, cand)


def test_results_match_accepts_percent_scale():
    assert results_match(pd.DataFrame({"r": [0.0933]}), pd.DataFrame({"pct": [9.33]}))


def test_results_mismatch_on_wrong_value_or_rowcount():
    ref = pd.DataFrame({"n": [400]})
    assert not results_match(ref, pd.DataFrame({"n": [447]}))
    assert not results_match(ref, pd.DataFrame({"n": [400, 1]}))


def test_question_file_is_valid():
    qs = load_questions(os.path.join(ROOT, "eval", "questions.yaml"))
    assert len(qs) == 30
    assert len({q["id"] for q in qs}) == 30
    for q in qs:
        check_sql(q["sql"])  # every reference query must itself pass the guardrails


# --- agent loop -------------------------------------------------------------
def test_agent_retries_after_error_and_after_guardrail_block():
    replies = iter([
        "```sql\nSELECT * FROM workspace.bronze.patients\n```",        # blocked
        "```sql\nSELECT bad_column FROM workspace.gold.dim_patient\n```",  # engine error
        "```sql\nSELECT COUNT(*) AS n FROM workspace.gold.dim_patient\n```",
        "There are 3 patients.",
    ])

    def run(sql):
        if "bad_column" in sql:
            raise RuntimeError("[UNRESOLVED_COLUMN] bad_column")
        return pd.DataFrame({"n": [3]})

    agent = SQLAgent(lambda m: next(replies), run, schema_context="stub")
    res = agent.ask("How many patients?")
    assert res.ok and res.attempts == 3
    assert res.answer == "There are 3 patients."
    assert "Blocked by guardrail" in res.trace[0]["error"]
