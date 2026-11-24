# Dashboard plan: Population Health and Cost

Built as a **Databricks AI/BI dashboard** (Dashboards > Create dashboard) on the Serverless Starter Warehouse.
Every dataset reads a small `gold.agg_*` or `gold.kpi_*` table, so the dashboard stays fast on a 2X-Small warehouse.

Synthea simulates full patient lifetimes, so encounter history goes back decades. Add a global **Date range filter** on `month_start` / `discharge_month` / `year`, defaulting to the **last 10 years**.

## Datasets

Create these in the **Data** tab of the dashboard editor.

| Dataset | SQL |
|---|---|
| `kpis` | `SELECT * FROM workspace.gold.kpi_overview` |
| `utilization` | `SELECT * FROM workspace.gold.agg_monthly_utilization` |
| `readmissions` | `SELECT * FROM workspace.gold.agg_readmission_monthly` |
| `payers` | `SELECT * FROM workspace.gold.agg_payer_performance` |
| `prevalence` | `SELECT * FROM workspace.gold.agg_chronic_prevalence` |
| `organizations` | `SELECT * FROM workspace.gold.agg_organization_performance` |
| `medications` | `SELECT * FROM workspace.gold.agg_top_medications` |
| `patient_cost` | `SELECT * FROM workspace.gold.agg_patient_cost_yearly` |
| `demographics` | `SELECT gender, age_band, race, income_band, is_deceased, COUNT(*) AS patients FROM workspace.gold.dim_patient GROUP BY ALL` |
| `dq` | `SELECT * FROM workspace.silver.dq_results WHERE run_ts = (SELECT MAX(run_ts) FROM workspace.silver.dq_results)` |

## Page 1: Executive overview

| Widget | Type | Fields |
|---|---|---|
| Living patients | Counter | `kpis.living_patients` |
| Encounters (last 12 months) | Counter | `kpis.encounters_last_12m` |
| Total cost | Counter | `kpis.total_cost` (currency) |
| Payer coverage rate | Counter | `kpis.payer_coverage_rate` (percent) |
| 30-day readmission rate | Counter | `kpis.readmission_rate_30d` (percent) |
| Encounters per month | Stacked area | x `month_start`, y `SUM(encounters)`, color `encounter_class` |
| Cost per month | Line | x `month_start`, y `SUM(total_cost)` and `SUM(payer_paid)` |
| Patients by age band and gender | Bar | x `age_band`, y `SUM(patients)`, color `gender` (filter `is_deceased = false`) |

## Page 2: Clinical quality

| Widget | Type | Fields |
|---|---|---|
| Readmission rate trend | Line | x `discharge_month`, y `readmission_rate_30d` |
| Average length of stay | Line | x `discharge_month`, y `avg_length_of_stay_days` |
| Chronic disease prevalence | Bar (horizontal) | y `condition_group`, x `SUM(patients_with_condition) / SUM(living_patients)` |
| Prevalence heatmap | Heatmap | x `age_band`, y `condition_group`, color `prevalence_rate` |
| Organizations by readmission rate | Table | `organization_name`, `inpatient_admissions`, `avg_length_of_stay_days`, `readmission_rate_30d` (filter admissions >= 20) |

## Page 3: Cost and payers

| Widget | Type | Fields |
|---|---|---|
| Cost by payer type | Bar | x `payer_type`, y `SUM(total_cost)`, `SUM(payer_paid)` |
| Coverage rate by payer | Bar | x `payer_name`, y `coverage_rate` (filter on latest year) |
| Patient out-of-pocket trend | Line | x `year`, y `SUM(patient_out_of_pocket)`, color `payer_type` |
| Spend per patient by payer type | Bar | x `year`, y `expenses_per_patient`, color `payer_type` (from `patient_cost`) |
| Top 10 medications by cost | Bar | y `medication_description`, x `SUM(total_cost)`, limit 10 |

## Page 4: Data quality

| Widget | Type | Fields |
|---|---|---|
| Checks passed | Counter | `COUNT(*)` where `status = 'PASS'` |
| Check results | Table | `table_name`, `check_name`, `severity`, `failed_rows`, `total_rows`, `status` |

## Publishing

1. Publish with **embedded credentials** so viewers don't need warehouse access.
2. Export each page as a PNG and add it to `docs/images/` in the repo. The README links to them.
3. Optional: schedule a daily refresh after the pipeline job finishes.
