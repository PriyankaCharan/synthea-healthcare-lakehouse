# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver: clean, type and conform the Synthea tables
# MAGIC
# MAGIC **Input:** workspace.bronze.* (raw CSVs loaded as-is from S3, header + inferred schema)
# MAGIC **Output:** workspace.silver.* (one clean table per entity) and workspace.silver.dq_results
# MAGIC
# MAGIC What this layer does:
# MAGIC 1. **Renames** every column to snake_case business names (PATIENT -> patient_id, TOTAL_CLAIM_COST -> total_claim_cost).
# MAGIC 2. **Casts explicitly** instead of trusting inferSchema (codes become strings, money becomes decimal(18,2), ZIPs get their leading zero back).
# MAGIC 3. **Removes direct identifiers** (SSN, driver's licence, passport, names, street address). Silver is the governed, analysis-ready layer, so PII stops at Bronze.
# MAGIC 4. **Standardises values** (gender M/F -> Male/Female, marital codes to labels, blank strings to NULL).
# MAGIC 5. **De-duplicates** on each table's business key.
# MAGIC 6. **Adds derived columns** that every downstream query needs (encounter duration, out-of-pocket cost, active flags, condition type).
# MAGIC 7. **Runs data quality checks** (keys, orphans, negative costs, time travel) and logs results to silver.dq_results.
# MAGIC
# MAGIC The notebook is idempotent: every table is fully rebuilt on each run.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
CATALOG = dbutils.widgets.get("catalog")
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
WRITE_FORMAT = "delta"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
print(f"Reading from {BRONZE}, writing to {SILVER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

from datetime import datetime
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

MONEY = "decimal(18,2)"


def bronze(table: str) -> DataFrame:
    return spark.table(f"{BRONZE}.{table}")


def col(df: DataFrame, *names: str, cast: str = None):
    """First of names that exists in df (Synthea renamed a few columns between
    releases, e.g. YEAR vs YEAR_DATE). Returns NULL if none exist so the
    pipeline never breaks on an optional column."""
    for n in names:
        if n in df.columns:
            c = F.col(n)
            if cast:
                c = c.cast(cast)
            return c
    return F.lit(None).cast(cast or "string")


def clean_str(c):
    """Trim and turn empty strings into NULL."""
    c = F.trim(c.cast("string"))
    return F.when(c == "", None).otherwise(c)


def code(df: DataFrame, *names: str):
    """Clinical codes (SNOMED, RxNorm, CVX...) and ZIP/FIPS are identifiers, not
    numbers. inferSchema reads them as bigint/double, so turn them back into
    clean strings without losing digits or adding '.0'."""
    c = col(df, *names)
    name = next((n for n in names if n in df.columns), None)
    if name is not None:
        dtype = dict(df.dtypes)[name]
        if dtype in ("double", "float") or dtype.startswith("decimal"):
            c = c.cast("decimal(38,0)")
    return clean_str(c.cast("string"))


def zip_code(df: DataFrame, *names: str):
    """ZIP/FIPS read as integers lose their leading zero (02139 -> 2139).
    Pad back to 5 digits; Synthea writes 0 when unknown, so map that to NULL."""
    z = F.lpad(code(df, *names), 5, "0")
    return F.when(z == "00000", None).otherwise(z)


def ts(c):
    return F.to_timestamp(c)


def dedupe(df: DataFrame, keys: list, order_col: str = None) -> DataFrame:
    if order_col is None:
        return df.dropDuplicates(keys)
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def write(df: DataFrame, name: str, comment: str) -> int:
    full = f"{SILVER}.{name}"
    df = df.withColumn("_silver_loaded_at", F.current_timestamp())
    (df.write.format(WRITE_FORMAT).mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(full))
    if WRITE_FORMAT == "delta":
        spark.sql(f"COMMENT ON TABLE {full} IS '{comment}'")
    n = spark.table(full).count()
    print(f"{full:<45} {n:>14,} rows")
    return n

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference tables: organization, provider, payer

# COMMAND ----------

b = bronze("organizations")
organization = dedupe(b.select(
    clean_str(col(b, "Id")).alias("organization_id"),
    clean_str(col(b, "NAME")).alias("organization_name"),
    clean_str(col(b, "ADDRESS")).alias("address"),
    clean_str(col(b, "CITY")).alias("city"),
    clean_str(col(b, "STATE")).alias("state"),
    zip_code(b, "ZIP").alias("zip"),
    col(b, "LAT", cast="double").alias("latitude"),
    col(b, "LON", cast="double").alias("longitude"),
    col(b, "REVENUE", cast=MONEY).alias("revenue"),
    col(b, "UTILIZATION", cast="int").alias("utilization"),
).filter("organization_id IS NOT NULL"), ["organization_id"])
write(organization, "organization", "Healthcare organizations (hospitals, clinics). One row per organization.")

b = bronze("providers")
provider = dedupe(b.select(
    clean_str(col(b, "Id")).alias("provider_id"),
    clean_str(col(b, "ORGANIZATION")).alias("organization_id"),
    clean_str(col(b, "NAME")).alias("provider_name"),
    F.when(col(b, "GENDER") == "M", "Male").when(col(b, "GENDER") == "F", "Female").alias("gender"),
    F.initcap(clean_str(col(b, "SPECIALITY", "SPECIALTY"))).alias("specialty"),
    clean_str(col(b, "CITY")).alias("city"),
    clean_str(col(b, "STATE")).alias("state"),
    zip_code(b, "ZIP").alias("zip"),
    col(b, "LAT", cast="double").alias("latitude"),
    col(b, "LON", cast="double").alias("longitude"),
).filter("provider_id IS NOT NULL"), ["provider_id"])
write(provider, "provider", "Clinicians. One row per provider, linked to their organization.")

b = bronze("payers")
payer = dedupe(b.select(
    clean_str(col(b, "Id")).alias("payer_id"),
    clean_str(col(b, "NAME")).alias("payer_name"),
    F.initcap(clean_str(col(b, "OWNERSHIP"))).alias("ownership"),
    clean_str(col(b, "STATE_HEADQUARTERED")).alias("state_headquartered"),
    col(b, "AMOUNT_COVERED", cast=MONEY).alias("amount_covered"),
    col(b, "AMOUNT_UNCOVERED", cast=MONEY).alias("amount_uncovered"),
    col(b, "REVENUE", cast=MONEY).alias("revenue"),
    col(b, "UNIQUE_CUSTOMERS", cast="int").alias("unique_customers"),
    col(b, "MEMBER_MONTHS", cast="bigint").alias("member_months"),
    col(b, "QOLS_AVG", cast="double").alias("avg_quality_of_life_score"),
).filter("payer_id IS NOT NULL"), ["payer_id"])
write(payer, "payer", "Insurance payers (Medicare, Medicaid, commercial, NO_INSURANCE). One row per payer.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Patient
# MAGIC Direct identifiers (SSN, DRIVERS, PASSPORT, PREFIX/FIRST/MIDDLE/LAST/SUFFIX/MAIDEN, ADDRESS) are deliberately dropped.

# COMMAND ----------

b = bronze("patients")
marital = (F.when(col(b, "MARITAL") == "M", "Married")
            .when(col(b, "MARITAL") == "S", "Single")
            .when(col(b, "MARITAL") == "D", "Divorced")
            .when(col(b, "MARITAL") == "W", "Widowed")
            .otherwise("Unknown"))

patient = dedupe(b.select(
    clean_str(col(b, "Id")).alias("patient_id"),
    F.to_date(col(b, "BIRTHDATE")).alias("birth_date"),
    F.to_date(col(b, "DEATHDATE")).alias("death_date"),
    F.when(col(b, "GENDER") == "M", "Male").when(col(b, "GENDER") == "F", "Female").otherwise("Unknown").alias("gender"),
    F.initcap(clean_str(col(b, "RACE"))).alias("race"),
    F.when(clean_str(col(b, "ETHNICITY")) == "hispanic", "Hispanic")
     .when(clean_str(col(b, "ETHNICITY")) == "nonhispanic", "Non-Hispanic")
     .otherwise("Unknown").alias("ethnicity"),
    marital.alias("marital_status"),
    clean_str(col(b, "BIRTHPLACE")).alias("birthplace"),
    clean_str(col(b, "CITY")).alias("city"),
    clean_str(col(b, "COUNTY")).alias("county"),
    clean_str(col(b, "STATE")).alias("state"),
    zip_code(b, "FIPS").alias("county_fips"),
    zip_code(b, "ZIP").alias("zip"),
    col(b, "LAT", cast="double").alias("latitude"),
    col(b, "LON", cast="double").alias("longitude"),
    col(b, "HEALTHCARE_EXPENSES", cast=MONEY).alias("lifetime_healthcare_expenses"),
    col(b, "HEALTHCARE_COVERAGE", cast=MONEY).alias("lifetime_healthcare_coverage"),
    col(b, "INCOME", cast=MONEY).alias("income"),
).filter("patient_id IS NOT NULL"), ["patient_id"])
patient = patient.withColumn("is_deceased", F.col("death_date").isNotNull())
write(patient, "patient", "De-identified patient demographics. One row per patient (living and deceased). PII removed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Encounter (the core event table)

# COMMAND ----------

b = bronze("encounters")
encounter = b.select(
    clean_str(col(b, "Id")).alias("encounter_id"),
    clean_str(col(b, "PATIENT")).alias("patient_id"),
    clean_str(col(b, "ORGANIZATION")).alias("organization_id"),
    clean_str(col(b, "PROVIDER")).alias("provider_id"),
    clean_str(col(b, "PAYER")).alias("payer_id"),
    F.lower(clean_str(col(b, "ENCOUNTERCLASS"))).alias("encounter_class"),
    code(b, "CODE").alias("encounter_code"),
    clean_str(col(b, "DESCRIPTION")).alias("encounter_description"),
    ts(col(b, "START")).alias("start_ts"),
    ts(col(b, "STOP")).alias("stop_ts"),
    col(b, "BASE_ENCOUNTER_COST", cast=MONEY).alias("base_encounter_cost"),
    col(b, "TOTAL_CLAIM_COST", cast=MONEY).alias("total_claim_cost"),
    col(b, "PAYER_COVERAGE", cast=MONEY).alias("payer_coverage"),
    code(b, "REASONCODE").alias("reason_code"),
    clean_str(col(b, "REASONDESCRIPTION")).alias("reason_description"),
).filter("encounter_id IS NOT NULL")

encounter = (dedupe(encounter, ["encounter_id"])
    .withColumn("encounter_date", F.to_date("start_ts"))
    .withColumn("duration_minutes",
                ((F.unix_timestamp("stop_ts") - F.unix_timestamp("start_ts")) / 60).cast("int"))
    .withColumn("patient_out_of_pocket",
                F.greatest(F.col("total_claim_cost") - F.col("payer_coverage"), F.lit(0).cast(MONEY)).cast(MONEY)))
write(encounter, "encounter", "Clinical encounters (visits, admissions, ED). One row per encounter, with cost and coverage.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical detail tables

# COMMAND ----------

def condition_type(desc):
    # Synthea descriptions end with the SNOMED semantic tag, e.g. "(disorder)", "(finding)"
    return F.regexp_extract(desc, r"\(([^()]+)\)\s*$", 1)


b = bronze("conditions")
condition = dedupe(b.select(
    clean_str(col(b, "PATIENT")).alias("patient_id"),
    clean_str(col(b, "ENCOUNTER")).alias("encounter_id"),
    clean_str(col(b, "SYSTEM")).alias("code_system"),
    code(b, "CODE").alias("condition_code"),
    clean_str(col(b, "DESCRIPTION")).alias("condition_description"),
    F.to_date(col(b, "START")).alias("onset_date"),
    F.to_date(col(b, "STOP")).alias("resolved_date"),
).filter("patient_id IS NOT NULL AND condition_code IS NOT NULL"),
    ["patient_id", "encounter_id", "condition_code", "onset_date"])
condition = (condition
    .withColumn("condition_type", condition_type(F.col("condition_description")))
    .withColumn("is_active", F.col("resolved_date").isNull()))
write(condition, "condition", "Diagnosed conditions (SNOMED). One row per patient, encounter, condition and onset date.")

b = bronze("medications")
medication = dedupe(b.select(
    clean_str(col(b, "PATIENT")).alias("patient_id"),
    clean_str(col(b, "PAYER")).alias("payer_id"),
    clean_str(col(b, "ENCOUNTER")).alias("encounter_id"),
    code(b, "CODE").alias("medication_code"),
    clean_str(col(b, "DESCRIPTION")).alias("medication_description"),
    ts(col(b, "START")).alias("start_ts"),
    ts(col(b, "STOP")).alias("stop_ts"),
    col(b, "BASE_COST", cast=MONEY).alias("base_cost"),
    col(b, "PAYER_COVERAGE", cast=MONEY).alias("payer_coverage"),
    col(b, "DISPENSES", cast="int").alias("dispenses"),
    col(b, "TOTALCOST", "TOTAL_COST", cast=MONEY).alias("total_cost"),
    code(b, "REASONCODE").alias("reason_code"),
    clean_str(col(b, "REASONDESCRIPTION")).alias("reason_description"),
).filter("patient_id IS NOT NULL AND medication_code IS NOT NULL"),
    ["patient_id", "encounter_id", "medication_code", "start_ts"])
medication = (medication
    .withColumn("start_date", F.to_date("start_ts"))
    .withColumn("is_active", F.col("stop_ts").isNull()))
write(medication, "medication", "Medication orders (RxNorm) with cost and payer coverage.")

b = bronze("allergies")
allergy = dedupe(b.select(
    clean_str(col(b, "PATIENT")).alias("patient_id"),
    clean_str(col(b, "ENCOUNTER")).alias("encounter_id"),
    code(b, "CODE").alias("allergy_code"),
    clean_str(col(b, "SYSTEM")).alias("code_system"),
    clean_str(col(b, "DESCRIPTION")).alias("allergy_description"),
    F.lower(clean_str(col(b, "TYPE"))).alias("allergy_type"),
    F.lower(clean_str(col(b, "CATEGORY"))).alias("allergy_category"),
    clean_str(col(b, "DESCRIPTION1")).alias("reaction_1"),
    F.lower(clean_str(col(b, "SEVERITY1"))).alias("severity_1"),
    F.to_date(col(b, "START")).alias("start_date"),
    F.to_date(col(b, "STOP")).alias("stop_date"),
).filter("patient_id IS NOT NULL"), ["patient_id", "allergy_code", "start_date"])
write(allergy, "allergy", "Patient allergies and intolerances.")

b = bronze("careplans")
careplan = dedupe(b.select(
    clean_str(col(b, "Id")).alias("careplan_id"),
    clean_str(col(b, "PATIENT")).alias("patient_id"),
    clean_str(col(b, "ENCOUNTER")).alias("encounter_id"),
    code(b, "CODE").alias("careplan_code"),
    clean_str(col(b, "DESCRIPTION")).alias("careplan_description"),
    code(b, "REASONCODE").alias("reason_code"),
    clean_str(col(b, "REASONDESCRIPTION")).alias("reason_description"),
    F.to_date(col(b, "START")).alias("start_date"),
    F.to_date(col(b, "STOP")).alias("stop_date"),
).filter("patient_id IS NOT NULL"), ["careplan_id", "careplan_code"])
write(careplan, "careplan", "Care plans (e.g. diabetes self-management) linked to the condition they treat.")

b = bronze("immunizations")
immunization = dedupe(b.select(
    clean_str(col(b, "PATIENT")).alias("patient_id"),
    clean_str(col(b, "ENCOUNTER")).alias("encounter_id"),
    code(b, "CODE").alias("vaccine_code"),
    clean_str(col(b, "DESCRIPTION")).alias("vaccine_description"),
    ts(col(b, "DATE")).alias("administered_ts"),
    col(b, "BASE_COST", "COST", cast=MONEY).alias("base_cost"),
).filter("patient_id IS NOT NULL"), ["patient_id", "encounter_id", "vaccine_code"])
immunization = immunization.withColumn("administered_date", F.to_date("administered_ts"))
write(immunization, "immunization", "Vaccines administered (CVX codes).")

b = bronze("devices")
device = dedupe(b.select(
    clean_str(col(b, "PATIENT")).alias("patient_id"),
    clean_str(col(b, "ENCOUNTER")).alias("encounter_id"),
    code(b, "CODE").alias("device_code"),
    clean_str(col(b, "DESCRIPTION")).alias("device_description"),
    clean_str(col(b, "UDI")).alias("udi"),
    ts(col(b, "START")).alias("start_ts"),
    ts(col(b, "STOP")).alias("stop_ts"),
).filter("patient_id IS NOT NULL"), ["patient_id", "encounter_id", "device_code", "udi"])
write(device, "device", "Implanted or assigned medical devices.")

b = bronze("imaging_studies")
imaging_study = dedupe(b.select(
    clean_str(col(b, "Id")).alias("imaging_study_id"),
    clean_str(col(b, "PATIENT")).alias("patient_id"),
    clean_str(col(b, "ENCOUNTER")).alias("encounter_id"),
    ts(col(b, "DATE")).alias("study_ts"),
    clean_str(col(b, "SERIES_UID")).alias("series_uid"),
    clean_str(col(b, "INSTANCE_UID")).alias("instance_uid"),
    clean_str(col(b, "BODYSITE_DESCRIPTION")).alias("body_site"),
    clean_str(col(b, "MODALITY_CODE")).alias("modality_code"),
    clean_str(col(b, "MODALITY_DESCRIPTION")).alias("modality"),
    code(b, "PROCEDURE_CODE").alias("procedure_code"),
).filter("patient_id IS NOT NULL"), ["imaging_study_id", "series_uid", "instance_uid"])
imaging_study = imaging_study.withColumn("study_date", F.to_date("study_ts"))
write(imaging_study, "imaging_study", "Imaging studies at instance level (modality, body site).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Financial tables: claim, patient_expense_yearly
# MAGIC claims.csv is the widest file. We keep the columns analysts use and give them readable names.

# COMMAND ----------

b = bronze("claims")
claim = dedupe(b.select(
    clean_str(col(b, "Id")).alias("claim_id"),
    clean_str(col(b, "PATIENTID")).alias("patient_id"),
    clean_str(col(b, "PROVIDERID")).alias("provider_id"),
    clean_str(col(b, "APPOINTMENTID")).alias("encounter_id"),
    clean_str(col(b, "PRIMARYPATIENTINSURANCEID")).alias("primary_payer_id"),
    clean_str(col(b, "SECONDARYPATIENTINSURANCEID")).alias("secondary_payer_id"),
    code(b, "DIAGNOSIS1").alias("primary_diagnosis_code"),
    F.to_date(col(b, "SERVICEDATE")).alias("service_date"),
    F.to_date(col(b, "CURRENTILLNESSDATE")).alias("current_illness_date"),
    F.upper(clean_str(col(b, "STATUS1"))).alias("primary_status"),
    F.upper(clean_str(col(b, "STATUS2"))).alias("secondary_status"),
    F.upper(clean_str(col(b, "STATUSP"))).alias("patient_status"),
    col(b, "OUTSTANDING1", cast=MONEY).alias("outstanding_primary"),
    col(b, "OUTSTANDING2", cast=MONEY).alias("outstanding_secondary"),
    col(b, "OUTSTANDINGP", cast=MONEY).alias("outstanding_patient"),
    col(b, "HEALTHCARECLAIMTYPEID1", cast="int").alias("claim_type_primary"),
).filter("claim_id IS NOT NULL"), ["claim_id"])
write(claim, "claim", "Insurance claims header. One row per claim; encounter_id links to silver.encounter.")

b = bronze("patient_expenses")
patient_expense_yearly = dedupe(b.select(
    clean_str(col(b, "PATIENT_ID", "PATIENT")).alias("patient_id"),
    F.year(F.to_date(col(b, "YEAR", "YEAR_DATE").cast("string"))).alias("year_from_date"),
    col(b, "YEAR", "YEAR_DATE", cast="string").alias("_year_raw"),
    clean_str(col(b, "PAYER_ID", "PAYER")).alias("payer_id"),
    col(b, "HEALTHCARE_EXPENSES", cast=MONEY).alias("healthcare_expenses"),
    col(b, "INSURANCE_COSTS", cast=MONEY).alias("insurance_costs"),
    col(b, "COVERED_COSTS", cast=MONEY).alias("covered_costs"),
).filter("patient_id IS NOT NULL")
 .withColumn("year", F.coalesce("year_from_date", F.substring("_year_raw", 1, 4).cast("int")))
 .drop("year_from_date", "_year_raw"),
    ["patient_id", "year", "payer_id"])
write(patient_expense_yearly, "patient_expense_yearly", "Yearly healthcare expenses and insurance costs per patient and payer.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks
# MAGIC Each check counts failing rows. Results are appended to silver.dq_results so quality can be tracked across runs (and shown on the dashboard).

# COMMAND ----------

run_ts = datetime.utcnow()
checks = [
    # (table, check name, SQL that returns the number of failing rows, severity)
    ("patient", "patient_id unique",
     f"SELECT COUNT(*) - COUNT(DISTINCT patient_id) FROM {SILVER}.patient", "error"),
    ("patient", "birth_date not null",
     f"SELECT COUNT(*) FROM {SILVER}.patient WHERE birth_date IS NULL", "error"),
    ("patient", "death_date after birth_date",
     f"SELECT COUNT(*) FROM {SILVER}.patient WHERE death_date < birth_date", "error"),
    ("encounter", "encounter_id unique",
     f"SELECT COUNT(*) - COUNT(DISTINCT encounter_id) FROM {SILVER}.encounter", "error"),
    ("encounter", "patient exists",
     f"SELECT COUNT(*) FROM {SILVER}.encounter e LEFT ANTI JOIN {SILVER}.patient p USING (patient_id)", "error"),
    ("encounter", "provider exists",
     f"SELECT COUNT(*) FROM {SILVER}.encounter e LEFT ANTI JOIN {SILVER}.provider p USING (provider_id)", "warn"),
    ("encounter", "payer exists",
     f"SELECT COUNT(*) FROM {SILVER}.encounter e LEFT ANTI JOIN {SILVER}.payer p USING (payer_id)", "warn"),
    ("encounter", "stop not before start",
     f"SELECT COUNT(*) FROM {SILVER}.encounter WHERE stop_ts < start_ts", "error"),
    ("encounter", "no negative cost",
     f"SELECT COUNT(*) FROM {SILVER}.encounter WHERE total_claim_cost < 0 OR payer_coverage < 0", "error"),
    ("encounter", "coverage not above cost",
     f"SELECT COUNT(*) FROM {SILVER}.encounter WHERE payer_coverage > total_claim_cost", "warn"),
    ("condition", "patient exists",
     f"SELECT COUNT(*) FROM {SILVER}.condition c LEFT ANTI JOIN {SILVER}.patient p USING (patient_id)", "error"),
    ("medication", "no negative cost",
     f"SELECT COUNT(*) FROM {SILVER}.medication WHERE total_cost < 0", "error"),
    ("claim", "claim_id unique",
     f"SELECT COUNT(*) - COUNT(DISTINCT claim_id) FROM {SILVER}.claim", "error"),
    ("claim", "encounter exists",
     f"SELECT COUNT(*) FROM {SILVER}.claim c LEFT ANTI JOIN {SILVER}.encounter e USING (encounter_id)", "warn"),
]

rows = []
for table, name, sql, severity in checks:
    failed = spark.sql(sql).collect()[0][0] or 0
    total = spark.table(f"{SILVER}.{table}").count()
    status = "PASS" if failed == 0 else ("FAIL" if severity == "error" else "WARN")
    rows.append((run_ts, table, name, severity, int(failed), int(total), status))

dq = spark.createDataFrame(
    rows, "run_ts timestamp, table_name string, check_name string, severity string, "
          "failed_rows bigint, total_rows bigint, status string")
dq.write.format(WRITE_FORMAT).mode("append").saveAsTable(f"{SILVER}.dq_results")
display(dq.orderBy("status", "table_name"))

failures = [r for r in rows if r[-1] == "FAIL"]
if failures:
    raise Exception(f"{len(failures)} blocking data quality check(s) failed: "
                    + "; ".join(f"{r[1]}.{r[2]} ({r[4]} rows)" for r in failures))
print("All blocking data quality checks passed.")
