"""Local smoke test (no Databricks needed).

Loads a small Synthea CSV export into a local "bronze" schema, then runs the
Silver notebook and the Gold SQL notebook end to end in local PySpark.
Databricks-only features (Delta, CLUSTER BY, comments, PK/FK) are skipped.

    java -jar synthea-with-dependencies.jar -p 400 -s 42 \
        --exporter.csv.export=true --exporter.fhir.export=false Massachusetts
    pip install pyspark
    python tests/run_local.py ./output/csv /tmp/spark-warehouse
"""
import os, re, sys, glob
from pyspark.sql import SparkSession

REPO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "notebooks")
CSV = sys.argv[1]
WH = sys.argv[2]

spark = (SparkSession.builder.master("local[4]")
         .config("spark.sql.warehouse.dir", WH)
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

BRONZE_FILES = ["allergies", "careplans", "claims", "conditions", "devices", "encounters",
                "imaging_studies", "immunizations", "medications", "organizations",
                "patient_expenses", "patients", "payers", "providers"]
for s in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {s}")
for f in BRONZE_FILES:
    (spark.read.option("header", "true").option("inferSchema", "true")
     .csv(f"{CSV}/{f}.csv").write.mode("overwrite").saveAsTable(f"bronze.{f}"))
print("bronze loaded")


class _W:
    def text(self, *a, **k): pass
    def get(self, name): return "spark_catalog"


class DBU: widgets = _W()


def display(df): df.show(30, truncate=False)


g = {"spark": spark, "dbutils": DBU(), "display": display}
silver_src = open(f"{REPO}/02_silver_transform.py").read().replace('WRITE_FORMAT = "delta"', 'WRITE_FORMAT = "parquet"')
exec(compile(silver_src, "silver", "exec"), g)

# ---- gold
src = open(f"{REPO}/03_gold_model.sql").read()
cells = src.split("-- COMMAND ----------")
for cell in cells:
    lines = [l for l in cell.strip().splitlines() if l.strip() != "-- Databricks notebook source"]
    if not lines:
        continue
    if lines[0].startswith("-- MAGIC %md"):
        continue
    if lines[0].startswith("-- MAGIC %python"):
        py = "\n".join(re.sub(r"^-- MAGIC ?", "", l) for l in lines[1:])
        exec(py, g)
        continue
    sql = "\n".join(lines)
    sql = re.sub(r"USE CATALOG \w+;", "", sql)
    sql = re.sub(r"CREATE OR REPLACE TABLE (\S+)\s*\nCOMMENT '[^']*'\s*\n(CLUSTER BY \([^)]*\)\s*\n)?AS",
                 r"DROP TABLE IF EXISTS \1;\nCREATE TABLE \1 USING parquet AS", sql)
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        if all(l.strip().startswith("--") for l in stmt.splitlines()):
            continue
        assert "CREATE OR REPLACE TABLE" not in stmt, stmt[:200]
        res = spark.sql(stmt)
        if stmt.lstrip().upper().startswith("SELECT"):
            res.show(truncate=False, vertical=True)

for t in [t for t in spark.catalog.listTables("gold") if not t.isTemporary]:
    print(f"gold.{t.name:<32}{spark.table('gold.'+t.name).count():>10,}")

