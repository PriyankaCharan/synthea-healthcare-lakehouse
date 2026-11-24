# Databricks notebook source
# MAGIC %md
# MAGIC # 04 SQL agent: ask the Gold layer in plain English
# MAGIC
# MAGIC A text-to-SQL agent built on a Databricks-hosted chat model (Model Serving).
# MAGIC
# MAGIC 1. **Context.** The prompt holds the business rules, the Gold schema (built from the table and column comments written by `03_gold_model`) and three worked examples.
# MAGIC 2. **Generate.** The model writes one Spark SQL query.
# MAGIC 3. **Guardrails.** Only a single read-only query over `workspace.gold` is allowed, and a row limit is always applied. Bronze and Silver (where PII lives) are unreachable.
# MAGIC 4. **Execute and self-correct.** If the query fails, the error goes back to the model for a fix (up to 3 attempts).
# MAGIC 5. **Explain.** The model turns the result into a short plain-English answer.
# MAGIC
# MAGIC The agent code lives in `agent/` and is shared with the evaluation notebook and the chat app.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.40.0" "openai>=1.40" "sqlglot>=25" pyyaml
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("endpoint", "databricks-qwen35-122b-a10b", "Model endpoint")
dbutils.widgets.text("catalog", "workspace")

import os
import sys

# Make the repo root importable (works both in a Git folder and in a bundle deployment).
repo_root = os.path.dirname(os.getcwd())  # notebooks run with their own folder as the working directory
sys.path.insert(0, repo_root)

from agent import SQLAgent, build_schema_context, databricks_llm, find_endpoint, spark_runner

CATALOG = dbutils.widgets.get("catalog")
ENDPOINT = find_endpoint(dbutils.widgets.get("endpoint"))
run_sql = spark_runner(spark)
schema = build_schema_context(run_sql, catalog=CATALOG)
agent = SQLAgent(databricks_llm(ENDPOINT), run_sql, schema, catalog=CATALOG)
print(f"Model endpoint: {ENDPOINT}")
print(f"Schema context: {len(schema):,} characters covering the Gold tables")

# COMMAND ----------

def ask(question: str):
    res = agent.ask(question)
    print(f"Q: {question}\n")
    if res.ok:
        print(res.answer, "\n")
        print(f"-- SQL (attempt {res.attempts}, {res.seconds}s)\n{res.sql}")
        display(res.data)
    else:
        print("Failed:", res.error)
        print(res.sql)
    return res

# COMMAND ----------

ask("What is the 30-day readmission rate for each payer type?")

# COMMAND ----------

ask("Which 5 organizations had the most inpatient admissions last year?")

# COMMAND ----------

ask("What share of living patients aged 65-79 have ever been diagnosed with diabetes, by gender?")

# COMMAND ----------

ask("How much did patients pay out of pocket for encounters in each of the last 5 years?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Guardrails in action
# MAGIC Requests that would read raw PII or change data are blocked before anything runs.

# COMMAND ----------

from agent import GuardrailError, check_sql

for sql in ["SELECT first, last, ssn FROM workspace.bronze.patients",
            "DROP TABLE workspace.gold.dim_patient"]:
    try:
        check_sql(sql)
    except GuardrailError as e:
        print(f"BLOCKED: {sql}\n   -> {e}\n")
