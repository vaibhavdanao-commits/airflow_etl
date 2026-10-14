from pathlib import Path
import os
import re
import json
import hashlib
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import sqlglot
from sqlglot import exp
from pyiceberg.catalog import load_catalog
from openai import OpenAI
from dotenv import load_dotenv


# ============================================================
# 1. CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
APP_FOLDER = Path(__file__).resolve().parent
WAREHOUSE_FOLDER = PROJECT_ROOT / "Warehouse"

# This is the catalog configuration requested from your reference script.
# Override these values in dashboard/.env if your paths differ.
load_dotenv(APP_FOLDER / ".env")
load_dotenv(PROJECT_ROOT / ".env", override=False)

CATALOG_DB = Path(
    os.getenv(
        "ICEBERG_CATALOG_DB",
        str(WAREHOUSE_FOLDER / "dimensional_model_catalog_test_v1.db"),
    )
).expanduser()
DIMENSIONAL_WAREHOUSE = Path(
    os.getenv("ICEBERG_WAREHOUSE", str(WAREHOUSE_FOLDER / "dimensional_model"))
).expanduser()
WAREHOUSE_URI = DIMENSIONAL_WAREHOUSE.resolve().as_uri()
NAMESPACE = tuple(
    part.strip()
    for part in os.getenv("ICEBERG_NAMESPACE", "star_schema").split(".")
    if part.strip()
)

MODEL_NAME = os.getenv("OLLAMA_MODEL", "llama3.2")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
MAX_RESULT_ROWS = int(os.getenv("MAX_RESULT_ROWS", "500"))
SQL_REPAIR_ATTEMPTS = int(os.getenv("SQL_REPAIR_ATTEMPTS", "2"))
AI_TIMEOUT_SECONDS = int(os.getenv("AI_TIMEOUT_SECONDS", "180"))

TABLES = {
    "dim_customer": (*NAMESPACE, "dim_customer"),
    "dim_product": (*NAMESPACE, "dim_product"),
    "dim_store": (*NAMESPACE, "dim_store"),
    "dim_date": (*NAMESPACE, "dim_date"),
    "fact_sales": (*NAMESPACE, "fact_sales"),
}
REQUIRED_TABLES = set(TABLES)

st.set_page_config(
    page_title="AI Retail Analytics",
    page_icon="📊",
    layout="wide",
)

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.4rem; padding-bottom: 2rem;}
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
    "Ask business questions about your existing Apache Iceberg star schema. "
    "Local Ollama generates SQL; DuckDB runs read-only queries; Plotly visualizes results."
)


# ============================================================
# 2. CONFIGURATION DIAGNOSTICS
# ============================================================

with st.sidebar:
    st.header("Warehouse connection")
    st.code(
        f"Project root: {PROJECT_ROOT}\n"
        f"Catalog DB: {CATALOG_DB}\n"
        f"Warehouse: {DIMENSIONAL_WAREHOUSE}\n"
        f"Namespace: {'.'.join(NAMESPACE)}\n"
        f"Ollama model: {MODEL_NAME}\n"
        f"Ollama endpoint: {OLLAMA_BASE_URL}",
        language="text",
    )
    st.caption(
        "Read-only dashboard. It loads Iceberg snapshots into in-memory DuckDB "
        "and does not create or modify source Iceberg tables."
    )

    if st.button("Reload Iceberg tables", use_container_width=True):
        initialize_database.clear()
        load_iceberg_catalog.clear()
        st.rerun()

    st.divider()
    st.subheader("Example questions")
    examples = [
        "Top 10 products by revenue",
        "Which cities have the highest sales?",
        "Show monthly revenue and month-over-month growth",
        "Which product categories have declining sales?",
        "Show sales by store",
        "Product_0094 revenue for every month",
        "Show Nagpur city sales for each month",
        "Show total units sold by product category",
        "Compare sales by payment method as a donut chart",
        "Show order status distribution as a pie chart",
    ]
    for i, example in enumerate(examples):
        if st.button(example, key=f"example_{i}", use_container_width=True):
            st.session_state["selected_example_question"] = example


# ============================================================
# 3. PYICEBERG CATALOG AND TABLE LOADING
# ============================================================

@st.cache_resource(show_spinner="Connecting to PyIceberg catalog...")
def load_iceberg_catalog():
    if not CATALOG_DB.exists():
        raise FileNotFoundError(
            f"Catalog database not found: {CATALOG_DB}\n"
            "Check ICEBERG_CATALOG_DB in your .env file."
        )
    if not DIMENSIONAL_WAREHOUSE.exists():
        raise FileNotFoundError(
            f"Dimensional warehouse folder not found: {DIMENSIONAL_WAREHOUSE}\n"
            "Check ICEBERG_WAREHOUSE in your .env file."
        )

    return load_catalog(
        "dimensional_model",
        type="sql",
        uri=f"sqlite:///{CATALOG_DB.as_posix()}",
        warehouse=WAREHOUSE_URI,
        schema_version="v1",
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"},
    )


@st.cache_resource(show_spinner="Loading Iceberg tables into in-memory DuckDB...")
def initialize_database():
    catalog = load_iceberg_catalog()
    namespaces = catalog.list_namespaces()

    if NAMESPACE not in namespaces:
        raise RuntimeError(
            f"Namespace {NAMESPACE} not found. Available namespaces: {namespaces}. "
            "Verify the catalog database and namespace."
        )

    existing = {identifier[-1] for identifier in catalog.list_tables(NAMESPACE)}
    missing = REQUIRED_TABLES - existing
    if missing:
        raise RuntimeError(
            f"Missing required Iceberg tables: {sorted(missing)}. "
            f"Tables found in {NAMESPACE}: {sorted(existing)}"
        )

    con = duckdb.connect(database=":memory:")
    loaded_tables = {}
    errors = []

    try:
        for alias, identifier in TABLES.items():
            try:
                iceberg_table = catalog.load_table(identifier)
                arrow_table = iceberg_table.scan().to_arrow()
                con.register(alias, arrow_table)
                loaded_tables[alias] = {
                    "identifier": ".".join(identifier),
                    "rows": arrow_table.num_rows,
                    "columns": arrow_table.column_names,
                }
            except Exception as exc:
                errors.append(f"{alias}: {type(exc).__name__}: {exc}")

        if not loaded_tables:
            raise RuntimeError("No Iceberg tables loaded.\n" + "\n".join(errors))

        # Disable external file access after the in-memory tables have been registered.
        try:
            con.execute("SET enable_external_access = false")
        except Exception:
            pass

        return con, loaded_tables, errors
    except Exception:
        con.close()
        raise


def get_schema(con, loaded_tables):
    sections = []
    for alias, metadata in loaded_tables.items():
        columns = con.execute(f'DESCRIBE SELECT * FROM "{alias}"').fetchall()
        column_details = "\n".join(f"- {row[0]} ({row[1]})" for row in columns)
        sections.append(
            f"TABLE: {alias}\n"
            f"ICEBERG IDENTIFIER: {metadata['identifier']}\n"
            f"ROWS LOADED: {metadata['rows']}\n"
            f"COLUMNS:\n{column_details}"
        )
    return "\n\n".join(sections)


# ============================================================
# 4. LOCAL OLLAMA CLIENT
# ============================================================

def get_ai_client():
    return OpenAI(
        base_url=OLLAMA_BASE_URL,
        api_key="ollama",  # Ollama does not require a paid OpenAI API key.
        timeout=AI_TIMEOUT_SECONDS,
        max_retries=1,
    )


def call_ai(messages, temperature=0):
    response = get_ai_client().chat.completions.create(
        model=MODEL_NAME,
        messages=messages,
        temperature=temperature,
    )
    return (response.choices[0].message.content or "").strip()


# ============================================================
# 5. NATURAL-LANGUAGE SQL GENERATION
# ============================================================

def clean_sql(text):
    text = (text or "").strip()
    text = re.sub(r"^```(?:sql)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    text = re.sub(r"^\s*SQL\s*:\s*", "", text, flags=re.IGNORECASE)
    return text.strip().rstrip(";").strip()


def generate_sql(question, schema, history):
    history_text = "\n".join(
        f"{item['role']}: {item['content']}" for item in history[-8:]
    )

    system_prompt = """
You are a careful senior retail data analyst and DuckDB SQL developer.

Return exactly ONE DuckDB SELECT query. Do not return Markdown,
explanations, comments, or multiple statements.

SECURITY:
- Query only registered tables in the supplied schema.
- Never read external files or call external services.
- Never generate INSERT, UPDATE, DELETE, CREATE, DROP, COPY, ATTACH,
  DETACH, INSTALL, LOAD, SET, PRAGMA, or other commands.
- Never invent a table, column, join key, or business metric.
- Use only actual column names in the supplied schema.
- Use explicit JOIN conditions.
- Use unqualified registered table names such as fact_sales.
- Do not use catalog-qualified table references.

RELATIONSHIPS (use only when these keys exist in the supplied schema):
fact_sales.customer_key = dim_customer.customer_key
fact_sales.product_key = dim_product.product_key
fact_sales.store_key = dim_store.store_key
fact_sales.date_key = dim_date.date_key
If these keys do not exist, inspect the supplied schema and use only
relationships supported by actual columns. Never guess a join key.

BUSINESS RULES:
1. Revenue means SUM(fact_sales.sales_amount) only if that column exists.
2. Units sold means SUM(fact_sales.quantity) only if it exists.
3. Product questions must filter the requested product using the actual
   product identifier or product name column.
4. City/customer questions must use the appropriate dimension column.
5. Date analysis must use actual date columns from dim_date or fact_sales.
6. "Monthly", "each month", or "every month" means one result row per month.
7. If data spans years, include year and month; do not combine January
   across years unless explicitly requested.
8. Order monthly results chronologically unless a ranking is requested.
9. Do not use LIMIT for every/all/each month, all dates, or complete trends.
10. Use ORDER BY and LIMIT for explicit top-N requests.
11. For month-over-month metrics, use LAG over chronological ordering.
12. Calculate percentages with an appropriate denominator and avoid divide-by-zero.
13. Avoid SELECT *; return useful dimensions and measures.
14. Use clear aliases such as revenue, total_sales, and total_units_sold.
15. Use DuckDB-compatible SQL syntax.
16. If the schema genuinely cannot answer the question, return
    SELECT 'INSUFFICIENT_SCHEMA' AS status
17. Preserve all requested filters, date ranges, grouping, and metrics.
18. Never invent data or claim unsupported relationships.
19. Apply product identifiers such as Product_0094 as an actual filter;
    never silently ignore a requested filter.
20. Before returning SQL, verify all columns, joins, filters, grouping,
    sorting, and requested metrics against the supplied schema.
"""

    prompt = f"""
ACTUAL DATABASE SCHEMA:
{schema}

RECENT CONVERSATION:
{history_text or "No previous conversation."}

USER QUESTION:
{question}

Return only the SQL query.
"""
    return clean_sql(
        call_ai(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )
    )


# ============================================================
# 6. SQL VALIDATION AND SAFE EXECUTION
# ============================================================

BLOCKED_FUNCTIONS = {
    "read_csv", "read_csv_auto", "read_parquet", "read_json", "read_text",
    "read_blob", "read_ndjson", "glob", "query", "query_table",
    "sqlite_scan", "postgres_scan", "parquet_scan", "csv_scan", "shell",
    "system", "set", "install", "load", "copy", "export", "import",
    "attach", "detach",
}


def validate_sql(sql, loaded_tables):
    statements = sqlglot.parse(sql, read="duckdb")
    if len(statements) != 1 or statements[0] is None:
        raise ValueError("Only one SQL statement is permitted.")

    tree = statements[0]
    if not isinstance(tree, (exp.Select, exp.Union)):
        raise ValueError("Only SELECT queries are permitted.")

    forbidden_nodes = (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create, exp.Alter, exp.Command)
    for node in tree.walk():
        if isinstance(node, forbidden_nodes):
            raise ValueError(f"Forbidden SQL operation: {type(node).__name__}")

        if isinstance(node, exp.Func):
            try:
                function_name = node.sql_name().lower()
            except Exception:
                function_name = ""
            if function_name in BLOCKED_FUNCTIONS:
                raise ValueError(f"Forbidden SQL function: {function_name}")

    allowed_tables = {name.lower() for name in loaded_tables}
    cte_names = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}

    for table in tree.find_all(exp.Table):
        table_name = table.name.lower()
        if table_name in cte_names:
            continue
        if table_name not in allowed_tables:
            raise ValueError(f"Table is not registered: {table.name}")
        if table.catalog:
            raise ValueError("Catalog-qualified table references are not allowed.")
        if table.db:
            if table.db.lower() != NAMESPACE[-1].lower():
                raise ValueError(f"Unknown schema reference: {table.db}")
            table.set("db", None)
        if not isinstance(table.this, exp.Identifier):
            raise ValueError("Table functions and external table sources are not permitted.")

    # Safety cap. If the query has no explicit limit, cap displayed rows.
    if not tree.args.get("limit"):
        tree = tree.limit(MAX_RESULT_ROWS)
    return tree.sql(dialect="duckdb")


def execute_sql(con, sql, loaded_tables):
    safe_sql = validate_sql(sql, loaded_tables)
    dataframe = con.execute(safe_sql).fetchdf()
    return dataframe, safe_sql


def generate_corrected_sql(question, schema, previous_sql, error_message):
    prompt = f"""
Correct the failed DuckDB SELECT query.

ACTUAL DATABASE SCHEMA:
{schema}

USER QUESTION:
{question}

FAILED SQL:
{previous_sql}

ERROR:
{error_message}

Rules:
- Return one SELECT query only.
- Use only actual schema columns and registered table names.
- Preserve requested filters, grouping, and output.
- For each month, return one row per month and sort chronologically.
- Do not invent columns or join keys.
- Do not return Markdown or explanations.
"""
    return clean_sql(
        call_ai(
            [
                {
                    "role": "system",
                    "content": "Repair DuckDB SQL using the supplied schema. Return one read-only SELECT statement.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )
    )


def run_query_with_repair(con, question, schema, history, loaded_tables):
    sql = generate_sql(question, schema, history)
    errors = []

    for attempt in range(SQL_REPAIR_ATTEMPTS + 1):
        try:
            dataframe, safe_sql = execute_sql(con, sql, loaded_tables)
            return dataframe, safe_sql, errors
        except Exception as exc:
            errors.append(f"Attempt {attempt + 1}: {type(exc).__name__}: {exc}")
            if attempt >= SQL_REPAIR_ATTEMPTS:
                raise RuntimeError(
                    "SQL execution failed after automatic repair.\n\n" + "\n".join(errors)
                ) from exc
            sql = generate_corrected_sql(question, schema, sql, str(exc))

    raise RuntimeError("Unable to execute SQL.")


# ============================================================
# 7. AI BUSINESS INSIGHTS
# ============================================================

def generate_insights(question, dataframe):
    if dataframe is None or dataframe.empty:
        return "No matching records were found."

    preview = dataframe.head(40).astype(object)
    preview = preview.where(preview.notna(), None)
    payload = {
        "columns": list(preview.columns),
        "rows": preview.to_dict(orient="records"),
        "rows_returned": len(dataframe),
        "preview_rows_sent": len(preview),
    }

    return call_ai(
        [
            {
                "role": "system",
                "content": (
                    "You are a retail analytics expert. Use only supplied SQL results. "
                    "Never invent numbers or claim causation without evidence. Explain "
                    "useful trends, comparisons, and limitations. If results may be "
                    "truncated, mention that limitation."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Question: {question}\n\nAnalyze these SQL results:\n"
                    + json.dumps(payload, default=str)
                ),
            },
        ],
        temperature=0.2,
    )


# ============================================================
# 8. INTERACTIVE VISUALIZATIONS
# ============================================================

def show_chart(dataframe, user_question=""):
    if dataframe is None or dataframe.empty:
        st.info("No data available for visualization.")
        return

    df = dataframe.copy()
    df.columns = [str(c).strip() for c in df.columns]
    question = str(user_question or "").lower()

    # Convert numeric-looking object columns when almost all non-null values are numeric.
    for col in df.columns:
        if df[col].dtype == object:
            cleaned = df[col].astype(str).str.replace(",", "", regex=False).str.strip()
            converted = pd.to_numeric(cleaned, errors="coerce")
            valid_count = df[col].notna().sum()
            if valid_count and converted.notna().sum() / valid_count >= 0.9:
                df[col] = converted

    datetime_columns = []
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            datetime_columns.append(col)
        elif any(token in col.lower() for token in ["date", "timestamp", "datetime"]):
            parsed = pd.to_datetime(df[col], errors="coerce")
            if len(df) and parsed.notna().sum() / len(df) >= 0.8:
                df[col] = parsed
                datetime_columns.append(col)

    numeric = df.select_dtypes(include="number").columns.tolist()
    categorical = [c for c in df.columns if c not in numeric and c not in datetime_columns]

    if not numeric:
        st.info("No numeric measures were returned. Choose a query that returns a numeric metric.")
        st.dataframe(df, use_container_width=True, hide_index=True)
        return

    priority = ["revenue", "sales_amount", "total_sales", "profit", "quantity", "units", "count"]
    def measure_rank(column):
        for idx, token in enumerate(priority):
            if token in column.lower():
                return idx
        return len(priority)

    numeric = sorted(numeric, key=measure_rank)
    default_y = numeric[0]
    combined_time = next(
        (c for c in df.columns if c.lower() in {"year_month", "yearmonth", "month_start"}),
        None,
    )
    default_x = combined_time or (datetime_columns[0] if datetime_columns else (categorical[0] if categorical else df.columns[0]))

    signature = hashlib.md5(
        ("|".join(df.columns) + "|" + str(user_question)).encode("utf-8")
    ).hexdigest()[:8]

    st.markdown("### 📈 Interactive visualizations")
    with st.expander("Chart controls", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            x_column = st.selectbox(
                "Category / Time", df.columns.tolist(),
                index=df.columns.tolist().index(default_x),
                key=f"chart_x_{signature}",
            )
        with c2:
            y_column = st.selectbox(
                "Numeric measure", numeric,
                index=numeric.index(default_y),
                key=f"chart_y_{signature}",
            )
        with c3:
            group_options = ["None"] + [c for c in df.columns if c not in {x_column, y_column}]
            group_column = st.selectbox("Breakdown", group_options, key=f"chart_group_{signature}")

        chart_type = st.selectbox(
            "Visualization type",
            ["Auto", "Bar", "Line", "Area", "Scatter", "Pie", "Donut",
             "Histogram", "Box", "Violin", "Treemap", "Sunburst", "Heatmap"],
            key=f"chart_type_{signature}",
        )
        additional_charts = st.checkbox("Show additional charts", value=True, key=f"additional_{signature}")

    if group_column == "None":
        group_column = None

    chart_source = df.copy()
    year_columns = [c for c in df.columns if c.lower() in {"year", "fiscal_year"}]
    if x_column.lower() in {"month", "month_name", "month_number"} and year_columns:
        month_map = {
            "january": 1, "february": 2, "march": 3, "april": 4,
            "may": 5, "june": 6, "july": 7, "august": 8,
            "september": 9, "october": 10, "november": 11, "december": 12,
        }
        month_numeric = pd.to_numeric(chart_source[x_column], errors="coerce")
        if not month_numeric.notna().all():
            month_numeric = chart_source[x_column].astype(str).str.lower().map(month_map)
        year_numeric = pd.to_numeric(chart_source[year_columns[0]], errors="coerce")
        if month_numeric.notna().all() and year_numeric.notna().all():
            chart_source["_chart_period"] = pd.to_datetime(
                {"year": year_numeric, "month": month_numeric, "day": 1},
                errors="coerce",
            )
            x_column = "_chart_period"

    keys = [x_column] + ([group_column] if group_column else [])
    try:
        chart_df = (
            chart_source.groupby(keys, dropna=False, as_index=False, observed=False)[y_column]
            .sum(min_count=1)
            .dropna(subset=[y_column])
        )
        if chart_df.empty:
            st.info("No valid numeric values are available to chart.")
            return

        is_time = (
            x_column in datetime_columns
            or x_column == "_chart_period"
            or pd.api.types.is_datetime64_any_dtype(chart_df[x_column])
            or any(t in x_column.lower() for t in ["date", "month", "period", "timestamp", "year_month"])
        )
        if is_time:
            chart_df = chart_df.sort_values(x_column)

        values = pd.to_numeric(chart_source[y_column], errors="coerce").dropna()
        k1, k2, k3 = st.columns(3)
        k1.metric(f"Returned {y_column} total", f"{values.sum():,.2f}")
        k2.metric(f"Highest returned {y_column}", f"{values.max():,.2f}" if not values.empty else "N/A")
        k3.metric("Rows returned", f"{len(df):,}")

        if chart_type == "Auto":
            if is_time or any(p in question for p in ["monthly", "each month", "every month", "trend", "over time", "daily", "weekly"]):
                chart_type = "Line"
            elif any(p in question for p in ["share", "contribution", "composition", "distribution"]):
                chart_type = "Donut"
            else:
                chart_type = "Bar"

        color = group_column
        if chart_type == "Line":
            fig = px.line(chart_df, x=x_column, y=y_column, color=color, markers=True, title=f"{y_column} over {x_column}")
        elif chart_type == "Bar":
            plot_df = chart_df if is_time else chart_df.sort_values(y_column, ascending=False)
            fig = px.bar(plot_df, x=x_column, y=y_column, color=color, title=f"{y_column} by {x_column}")
        elif chart_type == "Area":
            fig = px.area(chart_df, x=x_column, y=y_column, color=color, title=f"{y_column} over {x_column}")
        elif chart_type == "Scatter":
            fig = px.scatter(chart_df, x=x_column, y=y_column, color=color, title=f"{y_column} vs {x_column}")
        elif chart_type in {"Pie", "Donut"}:
            pie_df = chart_df.groupby(x_column, as_index=False, dropna=False, observed=False)[y_column].sum().sort_values(y_column, ascending=False).head(15)
            fig = px.pie(pie_df, names=x_column, values=y_column, hole=0.5 if chart_type == "Donut" else 0, title=f"{y_column} contribution")
        elif chart_type == "Histogram":
            fig = px.histogram(chart_source, x=y_column, nbins=30, title=f"Distribution of {y_column}")
        elif chart_type in {"Box", "Violin"}:
            if chart_type == "Box":
                fig = px.box(chart_source, x=group_column, y=y_column, points="outliers", title=f"{y_column} distribution")
            else:
                fig = px.violin(chart_source, x=group_column, y=y_column, box=True, points="outliers", title=f"{y_column} distribution")
        elif chart_type in {"Treemap", "Sunburst"}:
            hierarchy = list(dict.fromkeys([c for c in [group_column, x_column] if c and c != y_column]))
            if not hierarchy:
                st.info("Treemap and Sunburst require a category column.")
                return
            if chart_type == "Treemap":
                fig = px.treemap(chart_df, path=hierarchy, values=y_column, title=f"{y_column} by category")
            else:
                fig = px.sunburst(chart_df, path=hierarchy, values=y_column, title=f"{y_column} by hierarchy")
        elif chart_type == "Heatmap":
            if group_column:
                pivot = chart_df.pivot_table(index=group_column, columns=x_column, values=y_column, aggfunc="sum", fill_value=0, observed=False)
                fig = px.imshow(pivot, aspect="auto", title=f"{y_column} heatmap")
            elif len(numeric) >= 2:
                fig = px.imshow(chart_source[numeric].corr(), text_auto=".2f", zmin=-1, zmax=1, color_continuous_scale="RdBu_r", title="Numeric correlation heatmap")
            else:
                st.info("Heatmap requires a breakdown column or at least two numeric measures.")
                return
        else:
            fig = px.bar(chart_df, x=x_column, y=y_column, title=f"{y_column} by {x_column}")

        fig.update_layout(template="plotly_white", height=480, margin=dict(l=20, r=20, t=65, b=30))
        st.plotly_chart(fig, use_container_width=True, key=f"main_plot_{signature}")

        if additional_charts:
            st.markdown("#### Additional analysis")
            left, right = st.columns(2)
            with left:
                if group_column and is_time:
                    extra = px.area(chart_df, x=x_column, y=y_column, color=group_column, title=f"{y_column} trend by {group_column}")
                else:
                    top_df = chart_df.sort_values(y_column, ascending=False).head(12)
                    extra = px.bar(top_df, x=x_column, y=y_column, color=color, title=f"Top categories by {y_column}")
                extra.update_layout(template="plotly_white", height=380)
                st.plotly_chart(extra, use_container_width=True, key=f"extra_plot_{signature}")
            with right:
                distribution = px.histogram(chart_source, x=y_column, nbins=30, title=f"Distribution of {y_column}")
                distribution.update_layout(template="plotly_white", height=380)
                st.plotly_chart(distribution, use_container_width=True, key=f"distribution_plot_{signature}")

        with st.expander("View data used for charts"):
            st.dataframe(chart_source.drop(columns=["_chart_period"], errors="ignore"), use_container_width=True, hide_index=True)

    except Exception as exc:
        st.error(f"Unable to create visualizations: {exc}")
        with st.expander("Chart debugging details"):
            st.write("Columns:", df.columns.tolist())
            st.dataframe(df, use_container_width=True)


# ============================================================
# 9. INITIALIZE DASHBOARD
# ============================================================

try:
    con, loaded_tables, load_errors = initialize_database()
except Exception as exc:
    st.error("Unable to load the Iceberg dimensional model.")
    st.code(f"{type(exc).__name__}: {exc}")
    st.write("Catalog:", str(CATALOG_DB))
    st.write("Warehouse:", str(DIMENSIONAL_WAREHOUSE))
    st.write("Namespace:", ".".join(NAMESPACE))
    st.info(
        "Check the catalog path, namespace, PyIceberg configuration, table metadata paths, "
        "and the Python environment's native dependencies."
    )
    st.stop()

if load_errors:
    with st.expander("Table loading warnings"):
        for error in load_errors:
            st.warning(error)

st.subheader("Connected Iceberg Tables")
summary_columns = st.columns(max(1, len(loaded_tables)))
for column, (alias, metadata) in zip(summary_columns, loaded_tables.items()):
    column.metric(alias, f"{metadata['rows']:,} rows")

with st.expander("Configuration and catalog diagnostics"):
    st.write("Project root:", str(PROJECT_ROOT))
    st.write("App folder:", str(APP_FOLDER))
    st.write("Catalog database:", str(CATALOG_DB))
    st.write("Warehouse:", str(DIMENSIONAL_WAREHOUSE))
    st.write("Namespace:", ".".join(NAMESPACE))
    st.write("Ollama model:", MODEL_NAME)
    st.write("Ollama endpoint:", OLLAMA_BASE_URL)

with st.expander("View table schemas"):
    for alias, metadata in loaded_tables.items():
        st.markdown(f"**{alias}**")
        st.caption(metadata["identifier"])
        st.write(f"{metadata['rows']:,} rows")
        st.write(", ".join(metadata["columns"]))

schema_cols = sum(len(meta["columns"]) for meta in loaded_tables.values())
total_rows = sum(meta["rows"] for meta in loaded_tables.values())
m1, m2, m3, m4 = st.columns(4)
m1.metric("Iceberg tables", len(loaded_tables))
m2.metric("Total loaded rows", f"{total_rows:,}")
m3.metric("Total columns", schema_cols)
m4.metric("Namespace", ".".join(NAMESPACE))


# ============================================================
# 10. NATURAL-LANGUAGE QUESTION BOX
# ============================================================

for key, default in {
    "history": [],
    "result": None,
    "last_sql": None,
    "last_question": None,
    "last_sql_errors": [],
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

st.divider()
st.subheader("Ask your warehouse")

example_selected = st.session_state.pop("selected_example_question", "")
with st.form("question_form"):
    selected = st.selectbox(
        "Example questions",
        ["Select an example..."] + examples,
        index=(examples.index(example_selected) + 1) if example_selected in examples else 0,
    )
    default_question = selected if selected != "Select an example..." else example_selected
    question = st.text_area(
        "Enter your business question",
        value=default_question,
        placeholder="Example: Show Product_0094 revenue for every month",
        height=100,
    )
    submitted = st.form_submit_button("Analyze question", type="primary", use_container_width=True)

st.caption(
    "Uses local Ollama through its OpenAI-compatible API. No paid OpenAI API key is required. "
    "Start Ollama and download the configured model before analyzing."
)

if submitted:
    if not question.strip():
        st.warning("Please enter a question.")
    else:
        try:
            schema = get_schema(con, loaded_tables)
            with st.spinner("Generating SQL, validating it, and running analysis..."):
                dataframe, safe_sql, errors = run_query_with_repair(
                    con=con,
                    question=question.strip(),
                    schema=schema,
                    history=st.session_state.history,
                    loaded_tables=loaded_tables,
                )

            st.session_state.result = dataframe
            st.session_state.last_sql = safe_sql
            st.session_state.last_question = question.strip()
            st.session_state.last_sql_errors = errors
            st.session_state.history.extend(
                [
                    {"role": "user", "content": question.strip()},
                    {"role": "assistant", "content": safe_sql},
                ]
            )
            st.session_state.history = st.session_state.history[-12:]
        except Exception as exc:
            st.error("The analysis could not be completed.")
            st.code(f"{type(exc).__name__}: {exc}")
            if "errors" in locals() and errors:
                with st.expander("SQL repair attempts"):
                    for error in errors:
                        st.write(error)


# ============================================================
# 11. DISPLAY RESULTS
# ============================================================

if st.session_state.result is not None:
    dataframe = st.session_state.result
    st.divider()
    st.subheader("Query results")
    st.write("**Question:**", st.session_state.last_question)

    col1, col2, col3 = st.columns(3)
    col1.metric("Rows returned", f"{len(dataframe):,}")
    col2.metric("Columns", len(dataframe.columns))
    col3.metric("Numeric measures", len(dataframe.select_dtypes(include="number").columns))

    if len(dataframe) >= MAX_RESULT_ROWS:
        st.warning(
            f"The result reached the {MAX_RESULT_ROWS}-row safety cap. "
            "Results and charts may be incomplete."
        )

    with st.expander("View generated SQL", expanded=True):
        st.code(st.session_state.last_sql or "", language="sql")

    if st.session_state.last_sql_errors:
        with st.expander("SQL correction history"):
            for error in st.session_state.last_sql_errors:
                st.write(error)

    tab_chart, tab_data = st.tabs(["Visualization", "Result data"])
    with tab_chart:
        show_chart(dataframe, user_question=st.session_state.last_question)
    with tab_data:
        st.dataframe(dataframe, use_container_width=True, hide_index=True)
        st.download_button(
            "Download results as CSV",
            dataframe.to_csv(index=False).encode("utf-8-sig"),
            file_name="retail_analysis.csv",
            mime="text/csv",
        )

    st.subheader("AI business insights")
    if st.button("Generate insights", type="primary"):
        try:
            with st.spinner("Analyzing the query results with local Ollama..."):
                insights = generate_insights(st.session_state.last_question, dataframe)
            st.markdown(insights)
        except Exception as exc:
            st.error(f"Insight generation failed: {exc}")

with st.expander("Recent question history"):
    if not st.session_state.history:
        st.caption("No questions asked yet.")
    else:
        for item in st.session_state.history:
            if item["role"] == "user":
                st.markdown(f"**Question:** {item['content']}")
            else:
                st.code(item["content"], language="sql")

st.divider()
st.caption(
    "Security note: SQL is parsed and restricted to read-only SELECT queries against registered "
    "in-memory warehouse tables. Results are capped; source Iceberg tables are not modified."
)

