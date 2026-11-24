# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze: land the raw Synthea CSVs as Delta tables
# MAGIC
# MAGIC **Source:** `s3://<bucket>/synthea/raw/seed42_p50000/*.csv` (Unity Catalog external location)
# MAGIC **Output:** `workspace.bronze.<file name>` (one table per CSV)
# MAGIC
# MAGIC Bronze keeps the data exactly as delivered: original column names, no filtering, no business logic.
# MAGIC Two audit columns are added so every row can be traced back to its file and load time.
# MAGIC Every CSV found in the folder is loaded, so adding a file (for example `observations.csv`) needs no code change.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("source_path", "s3://priyanka-synthea-healthcare/synthea/raw/seed42_p50000/")
CATALOG = dbutils.widgets.get("catalog")
SOURCE = dbutils.widgets.get("source_path").rstrip("/") + "/"
BRONZE = f"{CATALOG}.bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")
files = sorted(f for f in dbutils.fs.ls(SOURCE) if f.name.endswith(".csv"))
display(files)

# COMMAND ----------

from pyspark.sql import functions as F

results = []
for f in files:
    table = f"{BRONZE}.{f.name[:-4]}"
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("escape", '"')        # RFC 4180 quoting used by Synthea
          .csv(f.path)
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_ingested_at", F.current_timestamp()))
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(table))
    n = spark.table(table).count()
    results.append((table, f.size, n))
    print(f"{table:<45} {n:>14,} rows")

display(spark.createDataFrame(results, "table string, file_bytes long, row_count long"))
