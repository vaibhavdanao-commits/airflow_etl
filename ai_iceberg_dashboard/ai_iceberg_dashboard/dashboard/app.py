from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
load_dotenv(HERE / ".env")
# Make project modules importable when launched from any working directory.
sys.path.insert(0, str(HERE))

from iceberg_reader import load_iceberg_tables, project_paths
from query_engine import generate_analysis, run_read_only_query
from visualizations import show_visualization

st.set_page_config(page_title="AI Retail Warehouse Dashboard", page_icon="📊", layout="wide")

st.markdown("""
<style>
.block-container {padding-top: 1.4rem; padding-bottom: 2rem;}
[data-testid="stMetric"] {border: 1px solid rgba(128,128,128,.25); padding: 14px; border-radius: 12px;}
</style>
""", unsafe_allow_html=True)

st.title("🤖 AI-Driven Dimensional Warehouse Dashboard")
st.caption("Ask a business question. The local AI generates read-only SQL against your existing Apache Iceberg star schema.")

with st.sidebar:
    st.header("Warehouse connection")
    root, catalog_db, warehouse_path, namespace = project_paths()
    st.code(f"Catalog: {catalog_db}\nWarehouse: {warehouse_path}\nNamespace: {namespace}", language="text")
    st.caption("This dashboard reads your existing warehouse. It does not create, overwrite, or append Iceberg data.")
    if st.button("Reload warehouse data", use_container_width=True):
        st.cache_resource.clear()
        st.rerun()
    st.divider()
    st.subheader("Example questions")
    examples = [
        "Show monthly sales revenue",
        "Show the top 10 products by revenue",
        "Compare sales by payment method as a donut chart",
        "Show sales by city as a bar chart",
        "Show order status distribution as a pie chart",
        "Show monthly revenue for Product_0094",
    ]
    for example in examples:
        if st.button(example, key=f"example_{examples.index(example)}", use_container_width=True):
            st.session_state["business_question"] = example

@st.cache_resource(show_spinner="Connecting to the existing Iceberg warehouse and loading tables...")
def get_warehouse():
    frames, info = load_iceberg_tables()
    connection = duckdb.connect(database=":memory:")
    connection.execute("SET threads = 4")
    for name, frame in frames.items():
        # Register actual warehouse data in memory; no sample data or persistent DuckDB file is created.
        connection.register(name, frame)
    return frames, info, connection

try:
    frames, warehouse_info, connection = get_warehouse()
except Exception as exc:
    st.error("Warehouse initialization failed.")
    st.code(str(exc))
    st.markdown("**Check these items:**")
    st.markdown("- The catalog database and warehouse path in `dashboard/.env` point to the existing dimensional warehouse.\n- The namespace is `star_schema`.\n- The current Python environment can import PyIceberg and its native dependencies.\n- Ollama is needed for AI question-to-SQL generation; warehouse loading errors must be resolved first.")
    st.stop()

frames_by_name = frames
schema_cols = sum(len(info["columns"]) for info in warehouse_info["tables"].values())
metric_cols = st.columns(4)
metric_cols[0].metric("Iceberg tables", len(frames_by_name))
metric_cols[1].metric("Total loaded rows", f"{sum(len(df) for df in frames_by_name.values()):,}")
metric_cols[2].metric("Total columns", schema_cols)
metric_cols[3].metric("Namespace", warehouse_info["namespace"])

with st.expander("Inspect available tables and columns"):
    for table_name, details in warehouse_info["tables"].items():
        st.markdown(f"**{table_name}** — {details['rows']:,} rows")
        st.code(", ".join(details["columns"]), language="text")

st.divider()
st.subheader("Ask your warehouse")
question = st.text_area(
    "Business question",
    key="business_question",
    placeholder="Example: Show monthly revenue for Product_0094 and explain the trend",
    height=95,
)
col1, col2 = st.columns([1, 4])
with col1:
    run_clicked = st.button("Analyze question", type="primary", use_container_width=True)
with col2:
    st.caption("Uses local Ollama via the OpenAI-compatible API. No paid OpenAI key is required.")

if run_clicked:
    if not question.strip():
        st.warning("Enter a business question first.")
    else:
        try:
            with st.spinner("Interpreting the question and generating SQL with local Ollama..."):
                analysis = generate_analysis(question.strip(), frames_by_name)
            st.session_state["last_analysis"] = analysis
            st.session_state["last_question"] = question.strip()
            st.session_state.pop("last_error", None)
        except Exception as exc:
            st.session_state["last_error"] = str(exc)

if st.session_state.get("last_error"):
    st.error("AI analysis failed.")
    st.code(st.session_state["last_error"])

analysis = st.session_state.get("last_analysis")
last_question = st.session_state.get("last_question", "")
if analysis:
    st.subheader(analysis.get("title") or "Analysis results")
    explanation = analysis.get("explanation") or "The AI did not provide an explanation. Review the SQL and result below."
    st.write(explanation)
    sql = str(analysis.get("sql", "")).strip()
    with st.expander("Generated SQL", expanded=True):
        st.code(sql or "No query was generated.", language="sql")

    if sql:
        try:
            with st.spinner("Validating SQL and querying the in-memory Iceberg data..."):
                result_df = run_read_only_query(connection, sql, list(frames_by_name.keys()))
            st.success(f"Query completed — {len(result_df):,} row(s) displayed (maximum {os.getenv('MAX_RESULT_ROWS', '500')}).")
            if not result_df.empty:
                # Helpful generic summary metrics without claiming a particular business definition.
                nums = result_df.select_dtypes(include="number").columns.tolist()
                if nums:
                    kpi_cols = st.columns(min(3, len(nums)))
                    for idx, col in enumerate(nums[:3]):
                        series = pd.to_numeric(result_df[col], errors="coerce")
                        kpi_cols[idx].metric(f"Sum of {col.replace('_', ' ')}", f"{series.sum():,.2f}")
            tab_chart, tab_data = st.tabs(["Visualization", "Result data"])
            with tab_chart:
                show_visualization(result_df, last_question, str(analysis.get("chart_type", "auto")), key_prefix="query_result")
            with tab_data:
                st.dataframe(result_df, use_container_width=True, hide_index=True)
                st.download_button(
                    "Download result as CSV",
                    data=result_df.to_csv(index=False).encode("utf-8-sig"),
                    file_name="warehouse_query_result.csv",
                    mime="text/csv",
                )
        except Exception as exc:
            st.error("The generated SQL could not be executed safely or failed against the available schema.")
            st.code(str(exc))
            st.info("Try a more specific question. Check the available columns above and confirm that the local AI model is running.")
    else:
        st.info(explanation)

st.divider()
st.caption("Security note: SQL is parsed and restricted to read-only queries against registered warehouse tables. Results are capped; the source Iceberg tables are never modified by this dashboard.")
