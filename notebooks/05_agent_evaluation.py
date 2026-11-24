# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Agent evaluation: how often is the SQL agent right?
# MAGIC
# MAGIC 30 questions in `eval/questions.yaml` each have a hand-written reference query.
# MAGIC For every model we compare the **result** of the agent's query with the reference result
# MAGIC (row count and values; column names and extra columns don't matter, numbers within 1%).
# MAGIC
# MAGIC The worked examples in the prompt are different from these questions, so the score measures
# MAGIC how well the agent generalises, not whether it memorised answers.
# MAGIC
# MAGIC Results are appended to `workspace.agent.eval_results` so accuracy can be tracked over time.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.40.0" "openai>=1.40" "sqlglot>=25" pyyaml
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("endpoints", "databricks-qwen35-122b-a10b,databricks-gpt-oss-120b", "Model endpoints to compare (comma separated)")
dbutils.widgets.text("catalog", "workspace")

import datetime
import os
import sys

repo_root = os.path.dirname(os.getcwd())  # notebooks run with their own folder as the working directory
sys.path.insert(0, repo_root)

import pandas as pd
from agent import SQLAgent, build_schema_context, databricks_llm, find_endpoint, spark_runner
from agent.evaluation import evaluate, load_questions

CATALOG = dbutils.widgets.get("catalog")
run_sql = spark_runner(spark)
schema = build_schema_context(run_sql, catalog=CATALOG)
questions = load_questions(os.path.join(repo_root, "eval", "questions.yaml"), catalog=CATALOG)
print(f"{len(questions)} evaluation questions")

# COMMAND ----------

run_ts = datetime.datetime.utcnow()
all_results = []
for key in [e.strip() for e in dbutils.widgets.get("endpoints").split(",") if e.strip()]:
    endpoint = find_endpoint(key)
    print(f"\n=== {endpoint} ===")
    agent = SQLAgent(databricks_llm(endpoint), run_sql, schema, catalog=CATALOG)
    df = evaluate(agent, questions, run_sql)
    df.insert(0, "model", endpoint)
    df.insert(0, "run_ts", run_ts)
    all_results.append(df)
    print(f"{endpoint}: {df.correct.sum()}/{len(df)} correct ({df.correct.mean():.0%})")

results = pd.concat(all_results, ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoreboard

# COMMAND ----------

summary = (results.groupby("model")
           .agg(accuracy=("correct", "mean"), correct=("correct", "sum"), questions=("id", "count"),
                avg_seconds=("seconds", "mean"), first_try=("attempts", lambda a: (a == 1).mean()))
           .round(3).reset_index().sort_values("accuracy", ascending=False))
display(summary)

# COMMAND ----------

display(results.pivot_table(index="category", columns="model", values="correct", aggfunc="mean").round(2).reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Misses (to improve the prompt or the Gold comments)

# COMMAND ----------

display(results[~results.correct][["model", "id", "question", "error", "agent_sql", "reference_sql"]])

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.agent")
(spark.createDataFrame(results.astype({"error": "string", "agent_sql": "string"}))
      .write.mode("append").option("mergeSchema", "true").saveAsTable(f"{CATALOG}.agent.eval_results"))
print(f"Appended {len(results)} rows to {CATALOG}.agent.eval_results")
