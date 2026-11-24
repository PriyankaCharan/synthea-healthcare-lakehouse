"""Generates dashboard/population_health_and_cost.lvdash.json, a Databricks AI/BI
dashboard over the Gold layer. Import it with Workspace > Import, or deploy it
with the Asset Bundle. Re-run this script after changing the layout."""
import json
import os

G = "workspace.gold"
SNAP_YEAR = f"(SELECT YEAR(snapshot_date) FROM {G}.kpi_overview)"
SNAP = f"(SELECT snapshot_date FROM {G}.kpi_overview)"

datasets = {
    "kpis": f"SELECT *, ROUND(total_cost / 1e9, 2) AS total_cost_bn FROM {G}.kpi_overview",
    "utilization": f"""SELECT * FROM {G}.agg_monthly_utilization
WHERE month_start >= add_months({SNAP}, -120)
  AND month_start < DATE_TRUNC('MONTH', {SNAP})""",
    "readmissions": f"""SELECT * FROM {G}.agg_readmission_monthly
WHERE discharge_month >= add_months({SNAP}, -120)
  AND discharge_month < DATE_TRUNC('MONTH', {SNAP})""",
    "demographics": f"""SELECT gender, age_band, COUNT(*) AS patients
FROM {G}.dim_patient WHERE NOT is_deceased GROUP BY ALL""",
    "prevalence_overall": f"""SELECT condition_group,
  SUM(patients_with_condition) AS patients_with_condition,
  ROUND(SUM(patients_with_condition) / (SELECT living_patients FROM {G}.kpi_overview), 4) AS prevalence_rate
FROM {G}.agg_chronic_prevalence GROUP BY condition_group""",
    "prevalence_age": f"""WITH pop AS (
  SELECT age_band, COUNT(*) AS living FROM {G}.dim_patient WHERE NOT is_deceased GROUP BY age_band)
SELECT c.condition_group, c.age_band, ROUND(SUM(c.patients_with_condition) / MAX(pop.living), 4) AS prevalence_rate
FROM {G}.agg_chronic_prevalence c JOIN pop USING (age_band)
WHERE c.condition_group IN ('Hypertension', 'Diabetes', 'Prediabetes', 'Obesity', 'Coronary heart disease', 'COPD')
GROUP BY ALL""",
    "organizations": f"""SELECT organization_name, city, inpatient_admissions, avg_length_of_stay_days,
  readmission_rate_30d, encounters, total_cost
FROM {G}.agg_organization_performance
WHERE inpatient_admissions >= 50
ORDER BY readmission_rate_30d DESC""",
    "payer_split": f"""SELECT payer_type, 'Paid by insurer' AS component, SUM(payer_paid) AS amount
FROM {G}.agg_payer_performance WHERE year = {SNAP_YEAR} - 1 GROUP BY ALL
UNION ALL
SELECT payer_type, 'Paid by patient', SUM(patient_out_of_pocket)
FROM {G}.agg_payer_performance WHERE year = {SNAP_YEAR} - 1 GROUP BY ALL""",
    "payer_coverage": f"""SELECT payer_name, payer_type, coverage_rate, patients_served
FROM {G}.agg_payer_performance WHERE year = {SNAP_YEAR} - 1""",
    "oop_trend": f"""SELECT MAKE_DATE(year, 1, 1) AS year_start, payer_type, SUM(patient_out_of_pocket) AS patient_out_of_pocket
FROM {G}.agg_payer_performance WHERE year BETWEEN {SNAP_YEAR} - 10 AND {SNAP_YEAR} - 1 GROUP BY ALL""",
    "top_meds": f"""SELECT medication_description, SUM(total_cost) AS total_cost, SUM(patients) AS patients
FROM {G}.agg_top_medications WHERE year BETWEEN {SNAP_YEAR} - 10 AND {SNAP_YEAR} - 1
GROUP BY medication_description ORDER BY total_cost DESC LIMIT 10""",
    "dq": """SELECT table_name, check_name, severity, failed_rows, total_rows, status
FROM workspace.silver.dq_results
WHERE run_ts = (SELECT MAX(run_ts) FROM workspace.silver.dq_results)""",
}

USD = {"type": "number-currency", "currencyCode": "USD", "abbreviation": "compact",
       "decimalPlaces": {"type": "max", "places": 2}}
PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}
NUM = {"type": "number-plain", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}
INT = {"type": "number-plain", "decimalPlaces": {"type": "exact", "places": 0}}
DEC2 = {"type": "number-plain", "decimalPlaces": {"type": "exact", "places": 2}}

_counter = 0


def wid():
    global _counter
    _counter += 1
    return f"w{_counter:03d}"


def text(md, x, y, w, h):
    return {"widget": {"name": wid(), "textbox_spec": md}, "position": {"x": x, "y": y, "width": w, "height": h}}


def counter(ds, col, title, fmt, x, y, w=1, h=2):
    f = f"sum({col})"
    return {"widget": {"name": wid(),
                       "queries": [{"name": "main_query", "query": {"datasetName": ds, "disaggregated": False,
                                    "fields": [{"name": f, "expression": f"SUM(`{col}`)"}]}}],
                       "spec": {"version": 2, "widgetType": "counter",
                                "encodings": {"value": {"fieldName": f, "displayName": title, "format": fmt}},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def chart(kind, ds, title, x_field, y_field, x, y, w, h, color=None, x_type="categorical",
          y_fmt=None, x_title=None, y_title=None, y_agg="SUM", horizontal=False, description=None):
    fields = []
    if x_type == "temporal":
        xf = f"monthly({x_field})"
        fields.append({"name": xf, "expression": f"DATE_TRUNC(\"MONTH\", `{x_field}`)"})
    else:
        xf = x_field
        fields.append({"name": xf, "expression": f"`{x_field}`"})
    yf = f"{y_agg.lower()}({y_field})"
    fields.append({"name": yf, "expression": f"{y_agg}(`{y_field}`)"})
    enc_x = {"fieldName": xf, "scale": {"type": x_type}, "displayName": x_title or x_field}
    enc_y = {"fieldName": yf, "scale": {"type": "quantitative"}, "displayName": y_title or y_field}
    if y_fmt:
        enc_y["format"] = y_fmt
    if x_title:
        enc_x["axis"] = {"title": x_title}
    if y_title:
        enc_y["axis"] = {"title": y_title}
    if horizontal:
        enc_x, enc_y = enc_y, enc_x
    enc = {"x": enc_x, "y": enc_y, "label": {"show": False}}
    if color:
        fields.append({"name": color, "expression": f"`{color}`"})
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color.replace("_", " ")}
    frame = {"showTitle": True, "title": title}
    if description:
        frame.update({"showDescription": True, "description": description})
    return {"widget": {"name": wid(),
                       "queries": [{"name": "main_query", "query": {"datasetName": ds, "disaggregated": False,
                                                                     "fields": fields}}],
                       "spec": {"version": 3, "widgetType": kind, "encodings": enc, "frame": frame}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def table(ds, title, cols, x, y, w, h):
    columns = []
    for i, (name, kind, fmt) in enumerate(cols):
        c = {"fieldName": name, "booleanValues": ["false", "true"], "imageUrlTemplate": "{{ @ }}",
             "imageTitleTemplate": "{{ @ }}", "imageWidth": "", "imageHeight": "", "linkUrlTemplate": "{{ @ }}",
             "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": True,
             "type": kind, "displayAs": "string" if kind == "string" else "number", "visible": True, "order": 100000 + i,
             "title": name.replace("_", " "), "allowSearch": kind == "string",
             "alignContent": "left" if kind == "string" else "right", "allowHTML": False,
             "highlightLinks": False, "useMonospaceFont": False, "preserveWhitespace": False}
        if fmt:
            c["numberFormat"] = fmt
        columns.append(c)
    return {"widget": {"name": wid(),
                       "queries": [{"name": "main_query", "query": {"datasetName": ds, "disaggregated": True,
                                    "fields": [{"name": n, "expression": f"`{n}`"} for n, _, _ in cols]}}],
                       "spec": {"version": 1, "widgetType": "table", "encodings": {"columns": columns},
                                "invisibleColumns": [], "allowHTMLByDefault": False, "itemsPerPage": 25,
                                "paginationSize": "default", "condensed": True, "withRowNumber": False,
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


overview = [
    text("# Population Health and Cost\nSynthetic Massachusetts population (Synthea, seed 42). "
         "Trends show the last 10 years up to the data snapshot date.", 0, 0, 6, 1),
    counter("kpis", "living_patients", "Living patients", INT, 0, 1),
    counter("kpis", "encounters_last_12m", "Encounters, last 12 months", INT, 1, 1),
    counter("kpis", "total_cost_bn", "Total cost, USD billions (all years)", DEC2, 2, 1),
    counter("kpis", "payer_coverage_rate", "Paid by insurers", PCT, 3, 1),
    counter("kpis", "readmission_rate_30d", "30-day readmission rate", PCT, 4, 1),
    counter("kpis", "avg_length_of_stay_days", "Avg inpatient stay (days)", NUM, 5, 1),
    chart("area", "utilization", "Encounters per month by type", "month_start", "encounters", 0, 3, 6, 6,
          color="encounter_class", x_type="temporal", x_title="Month", y_title="Encounters", y_fmt=NUM),
    chart("line", "utilization", "Monthly cost of care", "month_start", "total_cost", 0, 9, 3, 6,
          x_type="temporal", x_title="Month", y_title="Total cost", y_fmt=USD),
    chart("bar", "demographics", "Living patients by age band and gender", "age_band", "patients", 3, 9, 3, 6,
          color="gender", x_title="Age band", y_title="Patients", y_fmt=NUM),
]

clinical = [
    text("# Clinical quality\nReadmissions count only inpatient stays whose 30-day follow-up window is complete.",
         0, 0, 6, 1),
    chart("line", "readmissions", "30-day readmission rate by discharge month", "discharge_month",
          "readmission_rate_30d", 0, 1, 3, 6, x_type="temporal", x_title="Discharge month",
          y_title="Readmission rate", y_fmt=PCT, y_agg="AVG"),
    chart("line", "readmissions", "Average inpatient length of stay", "discharge_month",
          "avg_length_of_stay_days", 3, 1, 3, 6, x_type="temporal", x_title="Discharge month",
          y_title="Days", y_fmt=NUM, y_agg="AVG"),
    chart("bar", "prevalence_overall", "Chronic disease prevalence (living patients)", "condition_group",
          "prevalence_rate", 0, 7, 3, 7, x_title="Condition", y_title="Share of living patients", y_fmt=PCT,
          horizontal=True),
    chart("line", "prevalence_age", "Prevalence by age band, six most common conditions", "age_band",
          "prevalence_rate", 3, 7, 3, 7, color="condition_group", x_title="Age band",
          y_title="Share of living patients", y_fmt=PCT),
    table("organizations", "Organizations by 30-day readmission rate (50+ inpatient admissions)", [
        ("organization_name", "string", None), ("city", "string", None),
        ("inpatient_admissions", "integer", "0,0"), ("avg_length_of_stay_days", "float", "0.0"),
        ("readmission_rate_30d", "float", "0.0%"), ("encounters", "integer", "0,0"),
    ], 0, 14, 6, 7),
]

cost = [
    text("# Cost and payers\nPayer charts use the last full year before the snapshot date.", 0, 0, 6, 1),
    chart("bar", "payer_split", "Who pays: insurer vs patient, by payer type", "payer_type", "amount",
          0, 1, 3, 6, color="component", x_title="Payer type", y_title="Amount", y_fmt=USD),
    chart("bar", "payer_coverage", "Share of cost covered, by payer", "payer_name", "coverage_rate",
          3, 1, 3, 6, x_title="Payer", y_title="Coverage rate", y_fmt=PCT, y_agg="AVG", horizontal=True),
    chart("line", "oop_trend", "Patient out-of-pocket spend per year", "year_start", "patient_out_of_pocket",
          0, 7, 3, 6, color="payer_type", x_type="temporal", x_title="Year", y_title="Out of pocket", y_fmt=USD),
    chart("bar", "top_meds", "Top 10 medications by cost (last 10 years)", "medication_description",
          "total_cost", 3, 7, 3, 6, x_title="Medication", y_title="Total cost", y_fmt=USD, horizontal=True),
]

quality = [
    text("# Data quality\nResults of the latest Silver pipeline run. FAIL blocks the Gold rebuild; WARN is reported only.",
         0, 0, 6, 1),
    table("dq", "Data quality checks (latest run)", [
        ("status", "string", None), ("table_name", "string", None), ("check_name", "string", None),
        ("severity", "string", None), ("failed_rows", "integer", "0,0"), ("total_rows", "integer", "0,0"),
    ], 0, 1, 6, 8),
]

# Page headers get two rows so the subtitle is visible; push everything else down one row.
for page in (overview, clinical, cost, quality):
    for item in page:
        if "textbox_spec" in item["widget"]:
            item["position"]["height"] = 2
        else:
            item["position"]["y"] += 1

dashboard = {
    "datasets": [{"name": k, "displayName": k.replace("_", " "), "query": v} for k, v in datasets.items()],
    "pages": [
        {"name": "overview", "displayName": "Overview", "layout": overview},
        {"name": "clinical", "displayName": "Clinical quality", "layout": clinical},
        {"name": "cost", "displayName": "Cost and payers", "layout": cost},
        {"name": "quality", "displayName": "Data quality", "layout": quality},
    ],
}

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "population_health_and_cost.lvdash.json")
with open(out, "w") as f:
    json.dump(dashboard, f, indent=2)
print("wrote", out)
