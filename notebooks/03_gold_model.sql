-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 Gold: star schema and KPI marts
-- MAGIC
-- MAGIC **Input:** workspace.silver.*  **Output:** workspace.gold.*
-- MAGIC
-- MAGIC Gold serves two consumers:
-- MAGIC - **The AI/BI dashboard** reads the small agg_* and kpi_* tables, so every tile loads in seconds on a 2X-Small warehouse.
-- MAGIC - **The Genie agent (conversational SQL)** reads the star schema (dim_* + fact_*). Table and column comments plus primary/foreign keys (last cell) tell Genie how to join and what each field means.
-- MAGIC
-- MAGIC | Type | Tables |
-- MAGIC |---|---|
-- MAGIC | Dimensions | dim_date, dim_patient, dim_provider, dim_organization, dim_payer |
-- MAGIC | Facts | fact_encounter, fact_condition, fact_medication |
-- MAGIC | Marts | kpi_overview, agg_monthly_utilization, agg_readmission_monthly, agg_payer_performance, agg_chronic_prevalence, agg_organization_performance, agg_top_medications, agg_patient_cost_yearly |
-- MAGIC
-- MAGIC **Snapshot date.** Synthea simulates up to the day it was run, so "today" for this data is the latest encounter date, not the real current date. Ages and "last 12 months" are measured from that snapshot date.

-- COMMAND ----------

USE CATALOG workspace;
CREATE SCHEMA IF NOT EXISTS gold;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Foreign keys from an earlier run block CREATE OR REPLACE on the dimensions they point to.
-- MAGIC # Drop them first (ignored on the very first run, when the tables do not exist yet).
-- MAGIC FKS = {
-- MAGIC     "fact_encounter": ["fk_enc_patient", "fk_enc_provider", "fk_enc_org", "fk_enc_payer", "fk_enc_date"],
-- MAGIC     "fact_condition": ["fk_cond_patient", "fk_cond_date"],
-- MAGIC     "fact_medication": ["fk_med_patient", "fk_med_payer", "fk_med_date"],
-- MAGIC     "dim_provider": ["fk_provider_org"],
-- MAGIC }
-- MAGIC for table, fks in FKS.items():
-- MAGIC     for fk in fks:
-- MAGIC         try:
-- MAGIC             spark.sql(f"ALTER TABLE gold.{table} DROP CONSTRAINT IF EXISTS {fk}")
-- MAGIC         except Exception:
-- MAGIC             pass

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensions

-- COMMAND ----------

-- Snapshot date used for ages and rolling windows.
-- first_date covers condition onsets and medication starts that predate the first encounter.
CREATE OR REPLACE TEMPORARY VIEW v_snapshot AS
SELECT
  (SELECT MAX(encounter_date) FROM silver.encounter) AS snapshot_date,
  LEAST(
    (SELECT MIN(encounter_date) FROM silver.encounter),
    (SELECT MIN(onset_date)     FROM silver.condition),
    (SELECT MIN(start_date)     FROM silver.medication)
  ) AS first_date;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_date
COMMENT 'Calendar dimension covering every date between the first and last encounter. Join on date_key (yyyymmdd).'
AS
SELECT
  CAST(date_format(d, 'yyyyMMdd') AS INT)   AS date_key,
  d                                          AS full_date,
  YEAR(d)                                    AS year,
  QUARTER(d)                                 AS quarter,
  CONCAT(YEAR(d), '-Q', QUARTER(d))          AS year_quarter,
  MONTH(d)                                   AS month,
  date_format(d, 'MMMM')                     AS month_name,
  CAST(date_trunc('MONTH', d) AS DATE)       AS month_start,
  date_format(d, 'yyyy-MM')                  AS year_month,
  WEEKOFYEAR(d)                              AS week_of_year,
  DAYOFWEEK(d)                               AS day_of_week,
  date_format(d, 'EEEE')                     AS day_name,
  DAYOFWEEK(d) IN (1, 7)                     AS is_weekend
FROM (
  SELECT explode(sequence(first_date, snapshot_date, INTERVAL 1 DAY)) AS d
  FROM v_snapshot
);

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_patient
COMMENT 'One row per patient (living and deceased), de-identified. Age is measured at the snapshot date (or at death).'
AS
WITH base AS (
  SELECT
    p.*,
    FLOOR(months_between(COALESCE(p.death_date, s.snapshot_date), p.birth_date) / 12) AS age
  FROM silver.patient p
  CROSS JOIN v_snapshot s
)
SELECT
  patient_id,
  gender,
  race,
  ethnicity,
  marital_status,
  birth_date,
  death_date,
  is_deceased,
  CAST(age AS INT) AS age,
  CASE
    WHEN age < 18 THEN '0-17'
    WHEN age < 35 THEN '18-34'
    WHEN age < 50 THEN '35-49'
    WHEN age < 65 THEN '50-64'
    WHEN age < 80 THEN '65-79'
    ELSE '80+'
  END AS age_band,
  city,
  county,
  county_fips,
  state,
  zip,
  latitude,
  longitude,
  income,
  CASE
    WHEN income IS NULL      THEN 'Unknown'
    WHEN income <  25000     THEN '< 25K'
    WHEN income <  50000     THEN '25K-50K'
    WHEN income < 100000     THEN '50K-100K'
    WHEN income < 200000     THEN '100K-200K'
    ELSE '200K+'
  END AS income_band,
  lifetime_healthcare_expenses,
  lifetime_healthcare_coverage
FROM base;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_organization
COMMENT 'Healthcare organizations (hospitals and clinics) in Massachusetts.'
AS
SELECT organization_id, organization_name, city, state, zip, latitude, longitude, revenue, utilization
FROM silver.organization;

CREATE OR REPLACE TABLE gold.dim_provider
COMMENT 'Clinicians with their specialty and the organization they work for.'
AS
SELECT
  pr.provider_id,
  pr.provider_name,
  pr.gender,
  pr.specialty,
  pr.organization_id,
  o.organization_name,
  pr.city,
  pr.state
FROM silver.provider pr
LEFT JOIN silver.organization o USING (organization_id);

CREATE OR REPLACE TABLE gold.dim_payer
COMMENT 'Insurance payers. payer_type groups them into Medicare, Medicaid, Commercial, Dual eligible and Uninsured.'
AS
SELECT
  payer_id,
  payer_name,
  ownership,
  CASE
    WHEN UPPER(payer_name) = 'NO_INSURANCE'           THEN 'Uninsured'
    WHEN UPPER(payer_name) LIKE '%DUAL ELIGIBLE%'     THEN 'Dual eligible'
    WHEN UPPER(payer_name) LIKE '%MEDICARE%'          THEN 'Medicare'
    WHEN UPPER(payer_name) LIKE '%MEDICAID%'          THEN 'Medicaid'
    ELSE 'Commercial'
  END AS payer_type,
  state_headquartered,
  unique_customers,
  member_months
FROM silver.payer;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Facts
-- MAGIC
-- MAGIC **30-day readmission** (on inpatient encounters only):
-- MAGIC - is_readmission_30d: this admission started within 30 days of the patient's previous inpatient discharge.
-- MAGIC - has_readmission_30d: this admission was followed by another inpatient admission within 30 days of discharge.
-- MAGIC - Readmission rate = admissions with has_readmission_30d / all inpatient admissions. Discharges in the last 30 days of data are excluded from the rate, because their 30-day window is not complete.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.fact_encounter
COMMENT 'One row per encounter: who, where, when, why, what it cost and who paid. Includes length of stay and 30-day readmission flags for inpatient stays.'
CLUSTER BY (encounter_date)
AS
WITH inpatient AS (
  SELECT
    encounter_id,
    LAG(stop_ts)   OVER (PARTITION BY patient_id ORDER BY start_ts) AS prev_discharge_ts,
    LEAD(start_ts) OVER (PARTITION BY patient_id ORDER BY start_ts) AS next_admit_ts
  FROM silver.encounter
  WHERE encounter_class = 'inpatient'
)
SELECT
  e.encounter_id,
  e.patient_id,
  e.provider_id,
  e.organization_id,
  e.payer_id,
  CAST(date_format(e.encounter_date, 'yyyyMMdd') AS INT) AS date_key,
  e.encounter_date,
  e.start_ts,
  e.stop_ts,
  e.encounter_class,
  CASE e.encounter_class
    WHEN 'urgentcare' THEN 'Urgent care'
    WHEN 'snf'        THEN 'Skilled nursing'
    ELSE initcap(e.encounter_class)
  END AS encounter_class_label,
  e.encounter_code,
  e.encounter_description,
  e.reason_code,
  e.reason_description,
  e.duration_minutes,
  CASE WHEN e.encounter_class = 'inpatient'
       THEN ROUND(e.duration_minutes / 1440.0, 2) END        AS length_of_stay_days,
  CAST(FLOOR(months_between(e.encounter_date, p.birth_date) / 12) AS INT) AS patient_age_at_encounter,
  e.base_encounter_cost,
  e.total_claim_cost,
  e.payer_coverage,
  e.patient_out_of_pocket,
  e.encounter_class = 'inpatient'                            AS is_inpatient,
  e.encounter_class = 'emergency'                            AS is_emergency,
  COALESCE(i.prev_discharge_ts IS NOT NULL
           AND e.start_ts <= i.prev_discharge_ts + INTERVAL 30 DAYS, FALSE) AS is_readmission_30d,
  COALESCE(i.next_admit_ts IS NOT NULL
           AND i.next_admit_ts <= e.stop_ts + INTERVAL 30 DAYS, FALSE)      AS has_readmission_30d,
  (e.encounter_class = 'inpatient'
     AND e.stop_ts <= s.snapshot_date - INTERVAL 30 DAYS)    AS is_readmission_eligible
FROM silver.encounter e
LEFT JOIN inpatient i USING (encounter_id)
LEFT JOIN silver.patient p USING (patient_id)
CROSS JOIN v_snapshot s;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.fact_condition
COMMENT 'One row per diagnosed condition episode. condition_group maps SNOMED descriptions to common chronic disease groups (NULL for acute or other conditions).'
AS
SELECT
  c.patient_id,
  c.encounter_id,
  c.condition_code,
  c.condition_description,
  c.condition_type,
  CASE
    WHEN lower(c.condition_description) LIKE '%prediabetes%'                                   THEN 'Prediabetes'
    WHEN lower(c.condition_description) LIKE '%diabetes%'                                      THEN 'Diabetes'
    WHEN lower(c.condition_description) LIKE '%hypertension%'                                  THEN 'Hypertension'
    WHEN lower(c.condition_description) RLIKE 'hyperlipidemia|hypertriglyceridemia'            THEN 'Hyperlipidemia'
    WHEN lower(c.condition_description) LIKE '%chronic kidney disease%'                        THEN 'Chronic kidney disease'
    WHEN lower(c.condition_description) RLIKE 'chronic obstructive|emphysema'                  THEN 'COPD'
    WHEN lower(c.condition_description) LIKE '%asthma%'                                        THEN 'Asthma'
    WHEN lower(c.condition_description) LIKE '%heart failure%'                                 THEN 'Heart failure'
    WHEN lower(c.condition_description) RLIKE 'ischemic heart disease|coronary|myocardial infarction' THEN 'Coronary heart disease'
    WHEN lower(c.condition_description) RLIKE 'alzheimer|dementia'                             THEN 'Dementia'
    WHEN lower(c.condition_description) RLIKE 'depress'                                        THEN 'Depression'
    WHEN lower(c.condition_description) RLIKE 'obesity|body mass index 30|body mass index 40' THEN 'Obesity'
    WHEN lower(c.condition_description) RLIKE 'carcinoma|neoplasm|cancer|lymphoma|leukemia'    THEN 'Cancer'
  END AS condition_group,
  c.onset_date,
  CAST(date_format(c.onset_date, 'yyyyMMdd') AS INT) AS onset_date_key,
  c.resolved_date,
  c.is_active,
  CAST(FLOOR(months_between(c.onset_date, p.birth_date) / 12) AS INT) AS age_at_onset
FROM silver.condition c
LEFT JOIN silver.patient p USING (patient_id);

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.fact_medication
COMMENT 'One row per medication order with cost, payer coverage and patient share.'
AS
SELECT
  m.patient_id,
  m.encounter_id,
  m.payer_id,
  m.medication_code,
  m.medication_description,
  m.start_date,
  CAST(date_format(m.start_date, 'yyyyMMdd') AS INT) AS start_date_key,
  CAST(m.stop_ts AS DATE) AS stop_date,
  m.is_active,
  m.dispenses,
  m.base_cost,
  m.total_cost,
  m.payer_coverage,
  CAST(GREATEST(m.total_cost - m.payer_coverage, 0) AS DECIMAL(18,2)) AS patient_cost,
  m.reason_code,
  m.reason_description
FROM silver.medication m;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## KPI marts (dashboard-ready)

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.kpi_overview
COMMENT 'Single-row headline KPIs for the dashboard header.'
AS
SELECT
  (SELECT COUNT(*) FROM gold.dim_patient)                                   AS total_patients,
  (SELECT COUNT(*) FROM gold.dim_patient WHERE NOT is_deceased)             AS living_patients,
  (SELECT COUNT(*) FROM gold.dim_patient WHERE is_deceased)                 AS deceased_patients,
  COUNT(*)                                                                  AS total_encounters,
  SUM(CASE WHEN f.encounter_date > s.snapshot_date - INTERVAL 365 DAYS THEN 1 ELSE 0 END) AS encounters_last_12m,
  SUM(f.total_claim_cost)                                                   AS total_cost,
  SUM(f.payer_coverage)                                                     AS payer_paid,
  SUM(f.patient_out_of_pocket)                                              AS patient_out_of_pocket,
  ROUND(SUM(f.payer_coverage) / NULLIF(SUM(f.total_claim_cost), 0), 4)      AS payer_coverage_rate,
  ROUND(AVG(f.total_claim_cost), 2)                                         AS avg_cost_per_encounter,
  SUM(CASE WHEN f.is_inpatient THEN 1 ELSE 0 END)                           AS inpatient_admissions,
  ROUND(AVG(CASE WHEN f.is_inpatient THEN f.length_of_stay_days END), 2)    AS avg_length_of_stay_days,
  ROUND(SUM(CASE WHEN f.is_readmission_eligible AND f.has_readmission_30d THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN f.is_readmission_eligible THEN 1 ELSE 0 END), 0), 4) AS readmission_rate_30d,
  MIN(f.encounter_date)                                                     AS data_start_date,
  MAX(s.snapshot_date)                                                      AS snapshot_date
FROM gold.fact_encounter f
CROSS JOIN v_snapshot s;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.agg_monthly_utilization
COMMENT 'Encounters, unique patients and cost per month and encounter class.'
AS
SELECT
  CAST(date_trunc('MONTH', encounter_date) AS DATE) AS month_start,
  encounter_class_label                             AS encounter_class,
  COUNT(*)                                          AS encounters,
  COUNT(DISTINCT patient_id)                        AS unique_patients,
  SUM(total_claim_cost)                             AS total_cost,
  SUM(payer_coverage)                               AS payer_paid,
  SUM(patient_out_of_pocket)                        AS patient_out_of_pocket,
  ROUND(AVG(total_claim_cost), 2)                   AS avg_cost_per_encounter
FROM gold.fact_encounter
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.agg_readmission_monthly
COMMENT '30-day readmission rate by discharge month. Only admissions with a complete 30-day follow-up window are counted.'
AS
SELECT
  CAST(date_trunc('MONTH', stop_ts) AS DATE)              AS discharge_month,
  COUNT(*)                                                AS index_admissions,
  SUM(CASE WHEN has_readmission_30d THEN 1 ELSE 0 END)    AS readmissions_30d,
  ROUND(SUM(CASE WHEN has_readmission_30d THEN 1 ELSE 0 END) / COUNT(*), 4) AS readmission_rate_30d,
  ROUND(AVG(length_of_stay_days), 2)                      AS avg_length_of_stay_days
FROM gold.fact_encounter
WHERE is_readmission_eligible
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.agg_payer_performance
COMMENT 'Yearly encounters, members served, cost and coverage rate per payer.'
AS
SELECT
  YEAR(f.encounter_date)                                  AS year,
  py.payer_name,
  py.payer_type,
  COUNT(*)                                                AS encounters,
  COUNT(DISTINCT f.patient_id)                            AS patients_served,
  SUM(f.total_claim_cost)                                 AS total_cost,
  SUM(f.payer_coverage)                                   AS payer_paid,
  SUM(f.patient_out_of_pocket)                            AS patient_out_of_pocket,
  ROUND(SUM(f.payer_coverage) / NULLIF(SUM(f.total_claim_cost), 0), 4) AS coverage_rate
FROM gold.fact_encounter f
LEFT JOIN gold.dim_payer py USING (payer_id)
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.agg_chronic_prevalence
COMMENT 'Share of living patients who have ever been diagnosed with each chronic condition group, by gender and age band.'
AS
WITH population AS (
  SELECT gender, age_band, COUNT(*) AS living_patients
  FROM gold.dim_patient
  WHERE NOT is_deceased
  GROUP BY ALL
),
cases AS (
  SELECT c.condition_group, p.gender, p.age_band, COUNT(DISTINCT c.patient_id) AS patients_with_condition
  FROM gold.fact_condition c
  JOIN gold.dim_patient p USING (patient_id)
  WHERE c.condition_group IS NOT NULL AND NOT p.is_deceased
  GROUP BY ALL
)
SELECT
  c.condition_group,
  c.gender,
  c.age_band,
  c.patients_with_condition,
  pop.living_patients,
  ROUND(c.patients_with_condition / pop.living_patients, 4) AS prevalence_rate
FROM cases c
JOIN population pop USING (gender, age_band);

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.agg_organization_performance
COMMENT 'Per organization: volume, patients, cost, inpatient length of stay and readmission rate.'
AS
SELECT
  o.organization_name,
  o.city,
  COUNT(*)                                                         AS encounters,
  COUNT(DISTINCT f.patient_id)                                     AS unique_patients,
  SUM(f.total_claim_cost)                                          AS total_cost,
  ROUND(AVG(f.total_claim_cost), 2)                                AS avg_cost_per_encounter,
  SUM(CASE WHEN f.is_inpatient THEN 1 ELSE 0 END)                  AS inpatient_admissions,
  ROUND(AVG(CASE WHEN f.is_inpatient THEN f.length_of_stay_days END), 2) AS avg_length_of_stay_days,
  SUM(CASE WHEN f.is_emergency THEN 1 ELSE 0 END)                  AS emergency_visits,
  ROUND(SUM(CASE WHEN f.is_readmission_eligible AND f.has_readmission_30d THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN f.is_readmission_eligible THEN 1 ELSE 0 END), 0), 4) AS readmission_rate_30d
FROM gold.fact_encounter f
JOIN gold.dim_organization o USING (organization_id)
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.agg_top_medications
COMMENT 'Medication orders, patients and cost per medication and year.'
AS
SELECT
  YEAR(start_date)               AS year,
  medication_description,
  COUNT(*)                       AS orders,
  COUNT(DISTINCT patient_id)     AS patients,
  SUM(dispenses)                 AS dispenses,
  SUM(total_cost)                AS total_cost,
  SUM(payer_coverage)            AS payer_paid,
  SUM(patient_cost)              AS patient_cost
FROM gold.fact_medication
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.agg_patient_cost_yearly
COMMENT 'Yearly healthcare spend per payer type: total expenses, insurance premiums/costs and covered amount.'
AS
SELECT
  e.year,
  COALESCE(py.payer_type, 'Unknown')   AS payer_type,
  COUNT(DISTINCT e.patient_id)         AS patients,
  SUM(e.healthcare_expenses)           AS healthcare_expenses,
  SUM(e.insurance_costs)               AS insurance_costs,
  SUM(e.covered_costs)                 AS covered_costs,
  ROUND(SUM(e.healthcare_expenses) / NULLIF(COUNT(DISTINCT e.patient_id), 0), 2) AS expenses_per_patient
FROM silver.patient_expense_yearly e
LEFT JOIN gold.dim_payer py USING (payer_id)
GROUP BY ALL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Keys and column descriptions
-- MAGIC Informational primary and foreign keys are not enforced by Databricks, but Genie, the dashboard editor and BI tools use them to pick the right joins. Column comments become Genie's data dictionary.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC PKS = {
-- MAGIC     "dim_date": "date_key", "dim_patient": "patient_id", "dim_provider": "provider_id",
-- MAGIC     "dim_organization": "organization_id", "dim_payer": "payer_id", "fact_encounter": "encounter_id",
-- MAGIC }
-- MAGIC FK_DEFS = [
-- MAGIC     ("fact_encounter", "fk_enc_patient", "patient_id", "dim_patient"),
-- MAGIC     ("fact_encounter", "fk_enc_provider", "provider_id", "dim_provider"),
-- MAGIC     ("fact_encounter", "fk_enc_org", "organization_id", "dim_organization"),
-- MAGIC     ("fact_encounter", "fk_enc_payer", "payer_id", "dim_payer"),
-- MAGIC     ("fact_encounter", "fk_enc_date", "date_key", "dim_date"),
-- MAGIC     ("fact_condition", "fk_cond_patient", "patient_id", "dim_patient"),
-- MAGIC     ("fact_condition", "fk_cond_date", "onset_date_key", "dim_date"),
-- MAGIC     ("fact_medication", "fk_med_patient", "patient_id", "dim_patient"),
-- MAGIC     ("fact_medication", "fk_med_payer", "payer_id", "dim_payer"),
-- MAGIC     ("fact_medication", "fk_med_date", "start_date_key", "dim_date"),
-- MAGIC     ("dim_provider", "fk_provider_org", "organization_id", "dim_organization"),
-- MAGIC ]
-- MAGIC COLUMN_COMMENTS = {
-- MAGIC     "dim_patient": {
-- MAGIC         "age": "Age in whole years at the snapshot date, or at death for deceased patients",
-- MAGIC         "age_band": "Age group: 0-17, 18-34, 35-49, 50-64, 65-79, 80+",
-- MAGIC         "is_deceased": "TRUE if the patient died during the simulation",
-- MAGIC         "income_band": "Household income group",
-- MAGIC         "lifetime_healthcare_expenses": "Total healthcare spend over the patient lifetime (USD)",
-- MAGIC     },
-- MAGIC     "fact_encounter": {
-- MAGIC         "encounter_class": "Raw class: wellness, ambulatory, outpatient, inpatient, emergency, urgentcare, home, hospice, snf, virtual",
-- MAGIC         "encounter_class_label": "Display name of encounter_class",
-- MAGIC         "total_claim_cost": "Total billed cost of the encounter (USD)",
-- MAGIC         "payer_coverage": "Amount paid by the insurer (USD)",
-- MAGIC         "patient_out_of_pocket": "total_claim_cost minus payer_coverage, floored at 0 (USD)",
-- MAGIC         "length_of_stay_days": "Inpatient length of stay in days; NULL for non-inpatient encounters",
-- MAGIC         "is_readmission_30d": "Inpatient admission that started within 30 days of the previous inpatient discharge",
-- MAGIC         "has_readmission_30d": "Inpatient admission followed by another inpatient admission within 30 days of discharge",
-- MAGIC         "is_readmission_eligible": "Inpatient admission whose 30-day follow-up window is complete. Use as the denominator for readmission rate",
-- MAGIC         "patient_age_at_encounter": "Patient age in years on the encounter date",
-- MAGIC     },
-- MAGIC     "fact_condition": {
-- MAGIC         "condition_group": "Chronic disease group (Diabetes, Hypertension, COPD...). NULL for acute and other conditions",
-- MAGIC         "condition_type": "SNOMED semantic tag: disorder, finding, situation...",
-- MAGIC         "is_active": "TRUE if the condition has no resolved date",
-- MAGIC     },
-- MAGIC     "fact_medication": {
-- MAGIC         "total_cost": "Total cost of the order across all dispenses (USD)",
-- MAGIC         "patient_cost": "total_cost minus payer_coverage, floored at 0 (USD)",
-- MAGIC     },
-- MAGIC }
-- MAGIC
-- MAGIC if spark.catalog.currentCatalog() == "spark_catalog":
-- MAGIC     print("Local test run: skipping constraints and comments")
-- MAGIC else:
-- MAGIC     for table, pk in PKS.items():
-- MAGIC         spark.sql(f"ALTER TABLE gold.{table} ALTER COLUMN {pk} SET NOT NULL")
-- MAGIC         spark.sql(f"ALTER TABLE gold.{table} ADD CONSTRAINT pk_{table} PRIMARY KEY ({pk})")
-- MAGIC     for table, name, column, ref in FK_DEFS:
-- MAGIC         spark.sql(f"ALTER TABLE gold.{table} ADD CONSTRAINT {name} FOREIGN KEY ({column}) REFERENCES gold.{ref}")
-- MAGIC     for table, cols in COLUMN_COMMENTS.items():
-- MAGIC         for column, text in cols.items():
-- MAGIC             spark.sql(f"ALTER TABLE gold.{table} ALTER COLUMN {column} COMMENT '{text}'")
-- MAGIC     print("Keys and comments applied")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sanity check

-- COMMAND ----------

SELECT * FROM gold.kpi_overview;
