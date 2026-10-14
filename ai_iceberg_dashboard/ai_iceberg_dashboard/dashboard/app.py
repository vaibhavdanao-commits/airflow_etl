from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# ============================================================
# CONFIGURATION
# ============================================================

HERE = Path(__file__).resolve().parent
load_dotenv(HERE / ".env")
sys.path.insert(0, str(HERE))

from iceberg_reader import load_iceberg_tables, project_paths
from query_engine import generate_analysis, run_read_only_query
from visualizations import show_visualization


st.set_page_config(
    page_title="AI Retail Warehouse Dashboard",
    page_icon="📊",
    layout="wide",
)

st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.4rem;
        padding-bottom: 2rem;
    }
    [data-testid="stMetric"] {
        border: 1px solid rgba(128,128,128,.25);
        padding: 14px;
        border-radius: 12px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🤖 AI-Driven Dimensional Warehouse Dashboard")
st.caption(
    "Ask one business question. Local AI generates related SQL queries "
    "and displays multiple independent analytics dashboards."
)


# ============================================================
# WAREHOUSE CONNECTION
# ============================================================

@st.cache_resource(show_spinner="Loading the existing Iceberg warehouse...")
def get_warehouse():
    frames, info = load_iceberg_tables()

    connection = duckdb.connect(database=":memory:")
    connection.execute("SET threads = 4")

    for name, frame in frames.items():
        connection.register(name, frame)

    return frames, info, connection


# ============================================================
# SCHEMA HELPERS
# ============================================================

def find_table(frames: dict[str, pd.DataFrame], name: str):
    """Find a table without depending on letter casing."""
    for table_name, frame in frames.items():
        if table_name.lower() == name.lower():
            return table_name, frame
    return None, None


def get_columns(frame: pd.DataFrame | None) -> list[str]:
    if frame is None:
        return []
    return [str(column) for column in frame.columns]


def find_column(
    frame: pd.DataFrame | None,
    candidates: list[str],
) -> str | None:
    """Return the first matching available column."""
    columns = {column.lower(): column for column in get_columns(frame)}

    for candidate in candidates:
        if candidate.lower() in columns:
            return columns[candidate.lower()]

    return None


def get_warehouse_schema(frames: dict[str, pd.DataFrame]) -> str:
    """Create a concise schema description from loaded tables."""
    lines = []

    for table_name, frame in frames.items():
        lines.append(
            f"Table {table_name}: "
            + ", ".join(str(column) for column in frame.columns)
        )

    return "\n".join(lines)


# ============================================================
# RELATED QUESTION GENERATION
# ============================================================

def generate_related_questions(
    question: str,
    frames: dict[str, pd.DataFrame],
    max_related: int = 4,
) -> list[str]:
    """
    Ask the existing LLM to propose related business questions.

    The model receives the actual available schema. It returns questions,
    not SQL. Each question is then sent separately through generate_analysis()
    and the existing read-only SQL execution function.
    """

    schema = get_warehouse_schema(frames)

    prompt = f"""
You are a senior retail analytics business analyst.

Original user question:
{question}

Available warehouse schema:
{schema}

Generate exactly {max_related} useful, DISTINCT related business questions
that can be answered using only the available tables and columns.

Requirements:
- Stay relevant to the original question.
- Prefer complementary analysis, such as time trends, product/category
  breakdowns, top performers, location comparison, or customer segments.
- Use only dimensions and measures that exist in the schema.
- Use fact_sales and join dimensions only when appropriate.
- Do not assume payment_method, order_status, or any column exists unless
  it is listed in the schema.
- If the original question specifies a product, city, category, or date
  range, preserve that filter in related questions where appropriate.
- Do not repeat the original question.
- Return ONLY a valid JSON object in this format:
  {{"questions": ["question 1", "question 2"]}}
"""

    # Discover the LLM object exposed by the current query_engine module.
    # We intentionally use the existing generate_analysis interface for SQL.
    try:
        import query_engine

        llm_factory = getattr(query_engine, "make_llm", None)

        if llm_factory is None:
            # Fallback: derive related questions from schema without
            # requiring a second LLM interface.
            return generate_schema_based_questions(
                question,
                frames,
                max_related=max_related,
            )

        llm = llm_factory()

        response = llm.invoke(prompt)
        content = getattr(response, "content", response)

        if isinstance(content, list):
            content = "\n".join(
                str(part.get("text", "")) if isinstance(part, dict)
                else str(part)
                for part in content
            )

        import json
        import re

        text = str(content).strip()
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)

        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", text, flags=re.DOTALL)
            if not match:
                raise ValueError("The model did not return a JSON question list.")
            payload = json.loads(match.group(0))

        questions = payload.get("questions", [])

        if not isinstance(questions, list):
            raise ValueError("The related-question response was not a list.")

        results = []
        seen = {question.strip().lower()}

        for item in questions:
            if not isinstance(item, str):
                continue

            item = item.strip()

            if not item or item.lower() in seen:
                continue

            seen.add(item.lower())
            results.append(item)

            if len(results) >= max_related:
                break

        if results:
            return results

    except Exception as exc:
        st.warning(
            "AI related-question generation was unavailable. "
            f"Using schema-based questions instead. Details: {exc}"
        )

    return generate_schema_based_questions(
        question,
        frames,
        max_related=max_related,
    )


def generate_schema_based_questions(
    question: str,
    frames: dict[str, pd.DataFrame],
    max_related: int = 4,
) -> list[str]:
    """
    Deterministic fallback using dimensions actually present in the schema.
    This does not create data or assume unavailable columns.
    """

    fact_table, fact = find_table(frames, "fact_sales")

    if fact is None:
        return []

    product_table, product = find_table(frames, "dim_product")
    customer_table, customer = find_table(frames, "dim_customer")
    store_table, store = find_table(frames, "dim_store")
    date_table, date = find_table(frames, "dim_date")

    measure = find_column(
        fact,
        ["sales_amount", "revenue", "total_sales", "quantity"],
    )

    if not measure:
        numeric = fact.select_dtypes(include="number").columns.tolist()
        measure = numeric[0] if numeric else None

    if not measure:
        return []

    questions: list[str] = []
    original = question.strip().lower()

    def add(text: str):
        if (
            text.strip().lower() != original
            and text not in questions
            and len(questions) < max_related
        ):
            questions.append(text)

    product_name = find_column(product, ["product_name"])
    category = find_column(product, ["category"])
    city = find_column(customer, ["city"])
    segment = find_column(customer, ["customer_segment"])
    store_name = find_column(store, ["store_name", "store_id"])
    month = find_column(
        date,
        ["year_month", "month", "month_name", "full_date", "date"],
    )

    q = question.lower()

    # Prioritize related analyses based on the user's intent.
    if any(word in q for word in ("product", "category", "item")):
        if product is not None and category:
            add(
                f"Show total {measure} by product category. "
                f"Use {fact_table} and {product_table} with the actual "
                "matching product key."
            )
        if product is not None and product_name:
            add(
                f"Show the top 10 products by total {measure}. "
                f"Use {fact_table} and {product_table} with the actual "
                "matching product key."
            )

    if any(word in q for word in ("month", "monthly", "trend", "year", "date")):
        if date is not None and month:
            add(
                f"Show total {measure} by {month} in chronological order. "
                f"Use {fact_table} and {date_table} with the actual "
                "matching date key."
            )

    if any(word in q for word in ("city", "customer", "region")):
        if customer is not None and city:
            add(
                f"Show total {measure} by customer {city}. "
                f"Use {fact_table} and {customer_table} with the actual "
                "matching customer key."
            )
        if customer is not None and segment:
            add(
                f"Show total {measure} by customer {segment}. "
                f"Use {fact_table} and {customer_table} with the actual "
                "matching customer key."
            )

    # Fill remaining slots with available dimensions.
    if date is not None and month:
        add(
            f"Show total {measure} by {month} in chronological order. "
            f"Use {fact_table} and {date_table} with the actual date key."
        )

    if product is not None and category:
        add(
            f"Show total {measure} by product category. "
            f"Use {fact_table} and {product_table} with the actual product key."
        )

    if product is not None and product_name:
        add(
            f"Show the top 10 products by total {measure}. "
            f"Use {fact_table} and {product_table} with the actual product key."
        )

    if customer is not None and city:
        add(
            f"Show total {measure} by customer city. "
            f"Use {fact_table} and {customer_table} with the actual customer key."
        )

    if customer is not None and segment:
        add(
            f"Show total {measure} by customer segment. "
            f"Use {fact_table} and {customer_table} with the actual customer key."
        )

    if store is not None and store_name:
        add(
            f"Show total {measure} by {store_name}. "
            f"Use {fact_table} and {store_table} with the actual store key."
        )

    return questions[:max_related]


# ============================================================
# EXECUTE ONE BUSINESS QUESTION
# ============================================================

def execute_business_question(
    question: str,
    frames: dict[str, pd.DataFrame],
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, Any]:
    """
    Generate SQL for one question and execute it with the existing
    read-only query runner.
    """
    result: dict[str, Any] = {
        "question": question,
        "analysis": None,
        "sql": "",
        "data": None,
        "error": None,
    }

    try:
        analysis = generate_analysis(question, frames)
        result["analysis"] = analysis

        sql = str(analysis.get("sql", "")).strip()
        result["sql"] = sql

        if not sql:
            raise ValueError("The AI did not generate SQL.")

        data = run_read_only_query(
            connection,
            sql,
            list(frames.keys()),
        )

        result["data"] = data

    except Exception as exc:
        result["error"] = str(exc)

    return result


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:
    st.header("Warehouse connection")

    try:
        root, catalog_db, warehouse_path, namespace = project_paths()

        st.code(
            f"Catalog: {catalog_db}\n"
            f"Warehouse: {warehouse_path}\n"
            f"Namespace: {namespace}",
            language="text",
        )
    except Exception as exc:
        st.warning(f"Could not read warehouse configuration: {exc}")

    st.caption(
        "This dashboard reads your existing Iceberg warehouse. "
        "It does not create or modify Iceberg tables."
    )

    if st.button("Reload warehouse data", use_container_width=True):
        st.cache_resource.clear()
        st.rerun()

    st.divider()
    st.subheader("Example questions")

    examples = [
        "Show monthly sales revenue",
        "Show the top 10 products by revenue",
        "Show sales by product category",
        "Show sales by customer city",
        "Show customer segment performance",
        "Show monthly revenue for Product_0094",
    ]

    for index, example in enumerate(examples):
        if st.button(
            example,
            key=f"example_{index}",
            use_container_width=True,
        ):
            st.session_state["business_question"] = example


# ============================================================
# LOAD WAREHOUSE
# ============================================================

try:
    frames, warehouse_info, connection = get_warehouse()
except Exception as exc:
    st.error("Warehouse initialization failed.")
    st.code(str(exc))
    st.markdown(
        """
        Check:
        - The catalog database and warehouse paths in `dashboard/.env`.
        - The Iceberg namespace, expected to be `star_schema`.
        - PyIceberg and DuckDB dependencies in your active Python environment.
        - Whether all required warehouse tables can be read.
        """
    )
    st.stop()

frames_by_name = frames

# Use actual loaded frames for reliable schema information.
schema_cols = sum(len(frame.columns) for frame in frames_by_name.values())

metric_cols = st.columns(4)
metric_cols[0].metric("Iceberg tables", len(frames_by_name))
metric_cols[1].metric(
    "Total loaded rows",
    f"{sum(len(frame) for frame in frames_by_name.values()):,}",
)
metric_cols[2].metric("Total columns", schema_cols)
metric_cols[3].metric(
    "Namespace",
    str(warehouse_info.get("namespace", "star_schema")),
)

with st.expander("Inspect available tables and columns"):
    for table_name, frame in frames_by_name.items():
        st.markdown(f"**{table_name}** — {len(frame):,} rows")
        st.code(", ".join(str(c) for c in frame.columns), language="text")


# ============================================================
# USER QUESTION
# ============================================================

st.divider()
st.subheader("Ask your warehouse")

question = st.text_area(
    "Business question",
    key="business_question",
    placeholder=(
        "Example: Show monthly revenue for Product_0094 "
        "and explain its trend"
    ),
    height=95,
)

col1, col2 = st.columns([1, 4])

with col1:
    run_clicked = st.button(
        "Generate dashboards",
        type="primary",
        use_container_width=True,
    )

with col2:
    st.caption(
        "Local Ollama generates the SQL. Each related query runs separately "
        "through the existing read-only query validator."
    )


# ============================================================
# GENERATE MAIN + RELATED DASHBOARDS
# ============================================================

if run_clicked:
    original_question = question.strip()

    if not original_question:
        st.warning("Enter a business question first.")
    else:
        st.session_state.pop("dashboard_results", None)
        st.session_state.pop("dashboard_error", None)

        try:
            with st.spinner("Generating related business questions with AI..."):
                related = generate_related_questions(
                    original_question,
                    frames_by_name,
                    max_related=4,
                )

            all_questions = [original_question] + related

            st.session_state["generated_questions"] = all_questions
            results = []

            progress = st.progress(0.0)
            status = st.empty()

            for index, current_question in enumerate(all_questions):
                status.write(
                    f"Analyzing {index + 1}/{len(all_questions)}: "
                    f"{current_question}"
                )

                item = execute_business_question(
                    current_question,
                    frames_by_name,
                    connection,
                )
                results.append(item)

                progress.progress((index + 1) / len(all_questions))

            progress.empty()
            status.empty()

            st.session_state["dashboard_results"] = results
            st.session_state["dashboard_question"] = original_question

        except Exception as exc:
            st.session_state["dashboard_error"] = str(exc)


# ============================================================
# DISPLAY ALL INDEPENDENT QUERY RESULTS
# ============================================================

if st.session_state.get("dashboard_error"):
    st.error("Dashboard generation failed.")
    st.code(st.session_state["dashboard_error"])

dashboard_results = st.session_state.get("dashboard_results", [])
dashboard_question = st.session_state.get("dashboard_question", "")

if dashboard_results:
    successful = [
        item for item in dashboard_results
        if item.get("data") is not None
    ]
    failed = [
        item for item in dashboard_results
        if item.get("error")
    ]

    st.divider()
    st.header("📊 Multi-Query Analytics Dashboard")
    st.write(f"**Original question:** {dashboard_question}")

    summary_cols = st.columns(3)
    summary_cols[0].metric("Queries generated", len(dashboard_results))
    summary_cols[1].metric("Queries successful", len(successful))
    summary_cols[2].metric("Queries failed", len(failed))

    for index, item in enumerate(dashboard_results, start=1):
        analysis = item.get("analysis") or {}
        result_df = item.get("data")
        current_question = item.get("question", "")
        title = analysis.get("title") or current_question

        with st.container(border=True):
            st.subheader(f"{index}. {title}")
            st.caption(current_question)

            if item.get("error"):
                st.error("This query failed. Other dashboard panels remain available.")

                with st.expander("Error details"):
                    st.code(item["error"])

                if item.get("sql"):
                    with st.expander("Generated SQL"):
                        st.code(item["sql"], language="sql")

                continue

            if result_df is None:
                st.warning("No result was returned.")
                continue

            if analysis.get("explanation"):
                st.write(analysis["explanation"])

            with st.expander("View generated SQL"):
                st.code(item.get("sql", ""), language="sql")

            st.success(f"Query completed — {len(result_df):,} row(s) returned.")

            if result_df.empty:
                st.info("This query returned no rows.")
                continue

            # Each query has its own visualization and data tabs.
            tab_chart, tab_data = st.tabs(
                ["📈 Visualization", "📋 Result data"]
            )

            with tab_chart:
                show_visualization(
                    result_df,
                    current_question,
                    str(analysis.get("chart_type", "auto")),
                    key_prefix=f"dashboard_query_{index}",
                )

            with tab_data:
                st.dataframe(
                    result_df,
                    use_container_width=True,
                    hide_index=True,
                )

                st.download_button(
                    "Download this query as CSV",
                    data=result_df.to_csv(index=False).encode("utf-8-sig"),
                    file_name=f"warehouse_query_{index}.csv",
                    mime="text/csv",
                    key=f"download_dashboard_query_{index}",
                )

st.divider()
st.caption(
    "Security: every generated SQL statement is executed through the existing "
    "read-only query runner. The source Iceberg tables are not modified. "
    "Query result limits are controlled by your query_engine configuration."
)
