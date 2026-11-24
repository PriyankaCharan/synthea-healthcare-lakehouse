"""Health Data SQL Agent: a chat app over the Gold layer (Databricks App).

Ask a question in plain English; the agent writes Spark SQL with a
Databricks-hosted model, checks it, runs it on the SQL warehouse and explains
the result. Configuration comes from environment variables set by the bundle:
  SERVING_ENDPOINT        chat model endpoint name
  DATABRICKS_WAREHOUSE_ID SQL warehouse id
"""
import logging
import os
import sys

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from agent import SQLAgent, build_schema_context, databricks_llm, warehouse_runner  # noqa: E402

CATALOG = os.getenv("AGENT_CATALOG", "workspace")
ENDPOINT = os.getenv("SERVING_ENDPOINT", "")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")

EXAMPLES = [
    "What is the 30-day readmission rate for each payer type?",
    "Which 5 organizations had the most inpatient admissions last year?",
    "What share of living patients aged 65-79 have diabetes?",
    "How much did patients pay out of pocket each year for the last 5 years?",
    "What are the top 10 medications by total cost?",
]

logging.basicConfig(level=logging.INFO)
st.set_page_config(page_title="Health Data SQL Agent", page_icon="🩺", layout="wide")


@st.cache_resource(show_spinner="Reading the Gold layer schema...")
def get_agent() -> SQLAgent:
    run_sql = warehouse_runner(WAREHOUSE_ID)
    schema = build_schema_context(run_sql, catalog=CATALOG)
    return SQLAgent(databricks_llm(ENDPOINT), run_sql, schema, catalog=CATALOG)


MEASURE_HINTS = ("rate", "share", "pct", "percent", "prevalence", "avg", "average", "cost", "paid",
                 "amount", "total", "spend", "pocket")


def auto_chart(df: pd.DataFrame):
    """Chart one measure against the first label column (time on a line, categories as bars)."""
    if df is None or len(df) < 2 or df.shape[1] < 2:
        return
    numeric = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    labels = [c for c in df.columns if c not in numeric]
    time_cols = [c for c in df.columns if any(k in str(c).lower() for k in ("year", "month", "date"))]
    x = (time_cols or labels or [df.columns[0]])[0]
    measures = [c for c in numeric if c != x]
    if not measures:
        return
    hinted = [c for c in measures if any(h in str(c).lower() for h in MEASURE_HINTS)]
    y = hinted[0] if hinted else measures[-1]
    data = df[[x, y]].sort_values(x) if x in time_cols else df[[x, y]]
    data = data.set_index(x)
    st.caption(f"{y.replace('_', ' ')} by {str(x).replace('_', ' ')}")
    if x in time_cols:
        st.line_chart(data)
    elif len(df) <= 30:
        st.bar_chart(data)


def show_result(res):
    if res.ok:
        st.markdown((res.answer or "").replace("$", "\\$"))  # "$" would otherwise start LaTeX
        auto_chart(res.data)
        with st.expander(f"SQL and data ({len(res.data)} rows, {res.seconds}s, attempt {res.attempts})"):
            st.code(res.sql, language="sql")
            st.dataframe(res.data, use_container_width=True, hide_index=True)
    else:
        st.error(f"I couldn't answer that: {res.error}")
        if res.sql:
            st.code(res.sql, language="sql")


# ---------------------------------------------------------------- sidebar
with st.sidebar:
    st.header("🩺 Health Data SQL Agent")
    st.caption("Ask questions about 57,398 synthetic Massachusetts patients (Synthea). "
               "The agent writes SQL over the Gold star schema, checks it is read-only, "
               "runs it and explains the answer.")
    st.markdown(f"**Model:** `{ENDPOINT}`")
    st.markdown("**Try:**")
    for q in EXAMPLES:
        if st.button(q, use_container_width=True):
            st.session_state.pending = q
    if st.button("Clear conversation", type="secondary"):
        st.session_state.messages = []

if not ENDPOINT or not WAREHOUSE_ID:
    st.error("SERVING_ENDPOINT and DATABRICKS_WAREHOUSE_ID must be set (they come from the app resources).")
    st.stop()

st.session_state.setdefault("messages", [])

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        if m["role"] == "user":
            st.markdown(m["content"])
        else:
            show_result(m["result"])

question = st.chat_input("Ask about patients, encounters, costs, readmissions...") or st.session_state.pop("pending", None)
if question:
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    # last two answered questions give the model context for follow-ups ("and for women?")
    previous = st.session_state.messages[:-1]
    pairs = [(previous[i], previous[i + 1]) for i in range(0, len(previous) - 1, 2)
             if previous[i]["role"] == "user" and previous[i + 1]["role"] == "assistant"]
    history = []
    for user_msg, bot_msg in [p for p in pairs if p[1]["result"].ok][-2:]:
        history += [{"role": "user", "content": user_msg["content"]},
                    {"role": "assistant", "content": f"```sql\n{bot_msg['result'].sql}\n```"}]

    with st.chat_message("assistant"):
        with st.spinner("Writing and running SQL..."):
            res = get_agent().ask(question, history=history)
        show_result(res)
    st.session_state.messages.append({"role": "assistant", "result": res})
