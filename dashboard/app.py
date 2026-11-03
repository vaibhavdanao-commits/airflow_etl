
from __future__ import annotations

import sys
import html
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv


# ============================================================
# CONFIGURATION
# ============================================================

HERE = Path(__file__).resolve().parent

load_dotenv(HERE / ".env")
sys.path.insert(0, str(HERE))

from iceberg_reader import load_iceberg_tables, project_paths
from query_engine import (
    generate_analysis,
    live_schema_description,
    run_read_only_query,
)

# RAG is optional at startup.
try:
    from rag_engine import rebuild_index, retrieve_context

    RAG_AVAILABLE = True
    RAG_IMPORT_ERROR = ""

except Exception as rag_import_exc:

    rebuild_index = None
    retrieve_context = None
    RAG_AVAILABLE = False
    RAG_IMPORT_ERROR = str(rag_import_exc)


st.set_page_config(
    page_title="AI Retail Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# THEME
# ============================================================

st.markdown(
    """
    <style>
    @import url(
        'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap'
    );

    html, body, [class*="css"] {
        font-family: Inter, sans-serif;
    }

    .stApp {
        background: #F4F7FB;
        color: #17233C;
    }

    .block-container {
        max-width: 1500px;
        padding-top: 1.5rem;
        padding-bottom: 3rem;
    }

    #MainMenu, footer {
        visibility: hidden;
    }

    header[data-testid="stHeader"] {
        background: transparent;
    }

    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #101B35, #17294B);
        border-right: 1px solid #263858;
    }

    section[data-testid="stSidebar"] * {
        color: #E8EEFF !important;
    }

    section[data-testid="stSidebar"] .stButton button {
        background: #20345B;
        border: 1px solid #344A70;
        border-radius: 9px;
        color: white !important;
    }

    section[data-testid="stSidebar"] .stButton button:hover {
        background: #334F82;
    }

    .main-header {
        background: linear-gradient(
            115deg, #172B52, #344F9D 55%, #6555C7
        );
        padding: 30px 34px;
        border-radius: 18px;
        margin-bottom: 25px;
        box-shadow: 0 8px 24px rgba(39, 57, 115, .13);
    }

    .main-header h1 {
        color: white;
        font-size: 30px;
        font-weight: 800;
        margin: 0;
    }

    .main-header p {
        color: #DDE5FF;
        font-size: 14px;
        margin: 9px 0 0;
    }

    .header-tag {
        display: inline-block;
        background: rgba(255,255,255,.15);
        border: 1px solid rgba(255,255,255,.23);
        padding: 5px 12px;
        border-radius: 20px;
        color: white;
        font-size: 11px;
        font-weight: 700;
        margin-bottom: 12px;
    }

    .section-title {
        font-size: 19px;
        font-weight: 750;
        color: #17233C;
        margin-top: 20px;
        margin-bottom: 5px;
    }

    .section-description {
        color: #7B879B;
        font-size: 12px;
        margin-bottom: 17px;
    }

    [data-testid="stMetric"] {
        background: white;
        border: 1px solid #E5EAF3;
        border-radius: 14px;
        padding: 18px;
        box-shadow: 0 3px 12px rgba(29,47,83,.045);
    }

    [data-testid="stMetricLabel"] {
        color: #748198 !important;
        font-size: 12px !important;
        font-weight: 600 !important;
    }

    [data-testid="stMetricValue"] {
        color: #172B52 !important;
        font-size: 25px !important;
        font-weight: 800 !important;
    }

    .stButton > button,
    .stDownloadButton > button {
        border-radius: 9px;
        font-weight: 600;
        min-height: 40px;
    }

    .stButton > button[kind="primary"] {
        background: linear-gradient(110deg, #475FD6, #7159D9);
        color: white;
        border: none;
    }

    .stTextArea textarea {
        background: white !important;
        border: 1px solid #DCE3EF !important;
        border-radius: 11px !important;
        color: #17233C !important;
    }

    [data-testid="stExpander"] {
        border: 1px solid #E5EAF3;
        border-radius: 10px;
        background: white;
    }

    .footer-text {
        text-align: center;
        color: #929DB0;
        font-size: 11px;
        padding-top: 20px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# HEADER
# ============================================================

st.markdown(
    """
    <div class="main-header">
        <div class="header-tag">✦ AI POWERED ANALYTICS</div>

        <h1>Retail Intelligence Dashboard</h1>

        <p>
            Ask one business question and analyze your Iceberg
            warehouse through four automatically generated visualizations.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# WAREHOUSE CONNECTION
# ============================================================

@st.cache_resource(show_spinner="Connecting to Iceberg warehouse...")
def get_warehouse():

    frames, info = load_iceberg_tables()

    connection = duckdb.connect(database=":memory:")

    connection.execute("SET threads = 4")

    for name, frame in frames.items():
        connection.register(name, frame)

    return frames, info, connection


@st.cache_resource(show_spinner="Preparing warehouse knowledge for AI...")
def get_rag_resources(schema_text: str):

    if not RAG_AVAILABLE or rebuild_index is None:
        return None, 0

    store, indexed_count = rebuild_index(schema_text)

    return store, int(indexed_count or 0)


# ============================================================
# DATA HELPERS
# ============================================================

def get_dimension_columns(df: pd.DataFrame) -> list[str]:

    dimensions = []

    for column in df.columns:

        series = df[column]
        name = str(column).lower()

        is_identifier = (
            name.endswith("_id")
            or name.endswith("_key")
            or name in {"id", "key"}
        )

        if is_identifier:
            continue

        is_date = (
            pd.api.types.is_datetime64_any_dtype(series)
            or any(
                word in name
                for word in ("date", "month", "year", "quarter")
            )
        )

        is_categorical = (
            pd.api.types.is_object_dtype(series)
            or pd.api.types.is_string_dtype(series)
            or isinstance(series.dtype, pd.CategoricalDtype)
            or pd.api.types.is_bool_dtype(series)
        )

        if (
            is_date or is_categorical
        ) and series.nunique(dropna=True) > 1:

            dimensions.append(str(column))

    return dimensions


def get_measure_columns(df: pd.DataFrame) -> list[str]:

    measures = []

    for column in df.select_dtypes(include="number").columns:

        name = str(column).lower()

        if (
            name.endswith("_id")
            or name.endswith("_key")
            or name in {"id", "key"}
        ):
            continue

        measures.append(str(column))

    return measures


def get_default_measure(df: pd.DataFrame) -> str | None:

    measures = get_measure_columns(df)

    if not measures:
        return None

    preferred = [
        "monthly_revenue",
        "total_revenue",
        "sales_amount",
        "revenue",
        "total_sales",
        "total_spend",
        "average_order_value",
        "quantity",
        "total_quantity",
    ]

    for candidate in preferred:

        for column in measures:

            if candidate in column.lower():
                return column

    return measures[0]


def get_default_dimension(df: pd.DataFrame) -> str | None:

    dimensions = get_dimension_columns(df)

    if not dimensions:
        return None

    preferred = [
        "month",
        "date",
        "year",
        "quarter",
        "region",
        "category",
        "product",
        "city",
        "segment",
        "store",
    ]

    for candidate in preferred:

        for column in dimensions:

            if candidate in column.lower():
                return column

    return dimensions[0]


def prepare_chart_data(
    df: pd.DataFrame,
    dimension: str,
    measure: str,
) -> pd.DataFrame:

    working = df[[dimension, measure]].copy()

    working[measure] = pd.to_numeric(
        working[measure],
        errors="coerce",
    )

    working = working.dropna(
        subset=[dimension, measure]
    )

    if working.empty:
        return working

    grouped = (
        working.groupby(
            dimension,
            dropna=False,
            sort=False,
        )[measure]
        .sum()
        .reset_index()
    )

    return grouped


# ============================================================
# FOUR-VIEW CHART RENDERING
# ============================================================

BAR_COLOR = "#635BDB"
LINE_COLOR = "#16A6A1"


def render_chart(
    df: pd.DataFrame,
    dimension: str,
    measure: str,
    chart_type: str,
    chart_key: str,
):

    chart_df = prepare_chart_data(
        df,
        dimension,
        measure,
    )

    if chart_df.empty:

        st.info("No data is available for this visualization.")

        return chart_df

    if chart_type in ("Bar", "Pie", "Donut"):

        chart_df = chart_df.head(20)

    if chart_type == "Bar":

        display_df = chart_df.sort_values(
            measure,
            ascending=True,
        )

        fig = px.bar(
            display_df,
            x=measure,
            y=dimension,
            orientation="h",
            template="plotly_white",
            title=f"{measure} by {dimension}",
            text_auto=".2s",
        )

        fig.update_traces(
            marker_color=BAR_COLOR,
            textposition="outside",
        )

    elif chart_type == "Line":

        fig = px.line(
            chart_df,
            x=dimension,
            y=measure,
            markers=True,
            template="plotly_white",
            title=f"{measure} trend by {dimension}",
        )

        fig.update_traces(
            line=dict(
                color=LINE_COLOR,
                width=3,
            ),
            marker=dict(size=8),
        )

    elif chart_type == "Donut":

        fig = px.pie(
            chart_df,
            names=dimension,
            values=measure,
            hole=0.58,
            template="plotly_white",
            title=f"{measure} distribution by {dimension}",
        )

    else:

        fig = px.pie(
            chart_df,
            names=dimension,
            values=measure,
            hole=0,
            template="plotly_white",
            title=f"{measure} distribution by {dimension}",
        )

    fig.update_layout(
        height=390,
        margin=dict(
            l=20,
            r=20,
            t=65,
            b=30,
        ),
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(
            family="Inter, sans-serif",
            color="#263653",
        ),
        title_font_size=16,
        showlegend=chart_type in ("Pie", "Donut"),
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
        key=chart_key,
    )

    return chart_df


def show_four_views(
    df: pd.DataFrame,
    dimension: str,
    measure: str,
):

    st.markdown("### 📊 Four Visualization Views")

    st.caption(
        f"Automatically displaying {measure} by {dimension} "
        "using four different chart types."
    )

    # FIRST ROW: BAR + LINE

    col1, col2 = st.columns(2, gap="large")

    with col1:

        st.markdown("#### 1. Bar Chart")

        render_chart(
            df,
            dimension,
            measure,
            "Bar",
            "analytics_bar",
        )

    with col2:

        st.markdown("#### 2. Line Chart")

        render_chart(
            df,
            dimension,
            measure,
            "Line",
            "analytics_line",
        )

    st.divider()

    # SECOND ROW: DONUT + PIE

    col3, col4 = st.columns(2, gap="large")

    with col3:

        st.markdown("#### 3. Donut Chart")

        render_chart(
            df,
            dimension,
            measure,
            "Donut",
            "analytics_donut",
        )

    with col4:

        st.markdown("#### 4. Pie Chart")

        render_chart(
            df,
            dimension,
            measure,
            "Pie",
            "analytics_pie",
        )

    # CHART DATA

    with st.expander("📋 View chart data"):

        chart_data = prepare_chart_data(
            df,
            dimension,
            measure,
        )

        st.dataframe(
            chart_data,
            use_container_width=True,
            hide_index=True,
        )


# ============================================================
# EXECUTE ORIGINAL BUSINESS QUESTION
# ============================================================

def execute_business_question(
    question: str,
    frames: dict,
    connection: duckdb.DuckDBPyConnection,
    schema_text: str,
    rag_store: Any = None,
) -> dict[str, Any]:

    result = {
        "question": question,
        "analysis": None,
        "sql": "",
        "data": None,
        "error": None,
    }

    try:

        rag_context = ""

        if rag_store is not None and retrieve_context is not None:

            try:

                rag_context = retrieve_context(
                    rag_store,
                    question,
                    k=6,
                ) or ""

            except Exception as rag_exc:

                st.warning(
                    f"RAG retrieval was skipped: {rag_exc}"
                )

        analysis = generate_analysis(
            question=question,
            frames=frames,
            schema=schema_text,
            rag_context=rag_context,
        )

        if not isinstance(analysis, dict):

            raise ValueError(
                "The query engine did not return an analysis dictionary."
            )

        result["analysis"] = analysis

        sql = str(
            analysis.get("sql", "")
        ).strip()

        result["sql"] = sql

        if not sql:

            raise ValueError(
                "The AI did not generate SQL."
            )

        data = run_read_only_query(
            connection,
            sql,
            list(frames.keys()),
        )

        if not isinstance(data, pd.DataFrame):

            data = pd.DataFrame(data)

        result["data"] = data

    except Exception as exc:

        result["error"] = str(exc)

    return result


# ============================================================
# SESSION STATE
# ============================================================

def initialize_state():

    defaults = {
        "dashboard_result": None,
        "dashboard_question": "",
        "dashboard_error": None,
    }

    for key, value in defaults.items():

        if key not in st.session_state:
            st.session_state[key] = value


initialize_state()


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:

    st.markdown(
        """
        <div style="padding:10px 0 20px 0;">

            <div style="font-size:28px;">📊</div>

            <div style="font-size:19px;font-weight:800;">
                Retail Intelligence
            </div>

            <div style="font-size:11px;color:#AAB9D6;">
                AI Analytics Platform
            </div>

        </div>
        """,
        unsafe_allow_html=True,
    )

    st.divider()

    st.markdown("### 🗄️ Warehouse")

    try:

        root, catalog_db, warehouse_path, namespace = project_paths()

        st.caption("Catalog database")

        st.code(
            str(catalog_db),
            language=None,
        )

        st.caption("Namespace")

        st.code(
            str(namespace),
            language=None,
        )

    except Exception as exc:

        st.warning(
            f"Configuration unavailable: {exc}"
        )

    st.divider()

    st.markdown("### ⚡ Actions")

    if st.button(
        "🔄 Reload Warehouse",
        use_container_width=True,
    ):

        st.cache_resource.clear()
        st.rerun()

    if st.button(
        "🧹 Clear Dashboard",
        use_container_width=True,
    ):

        st.session_state["dashboard_result"] = None
        st.session_state["dashboard_question"] = ""
        st.session_state["dashboard_error"] = None
        st.session_state["business_question"] = ""

        st.rerun()

    st.divider()

    st.markdown("### 💡 Example Questions")

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

    st.info(
        """
        Verify your catalog database, Iceberg warehouse path,
        namespace, PyIceberg dependencies and warehouse tables.
        """
    )

    st.stop()


# ============================================================
# LIVE SCHEMA
# ============================================================

try:

    LIVE_SCHEMA_TEXT = live_schema_description(
        connection,
        frames,
    )

except Exception as schema_exc:

    st.error(
        "Unable to inspect the live warehouse schema."
    )

    st.code(str(schema_exc))

    st.stop()


# ============================================================
# RAG INITIALIZATION
# ============================================================

RAG_STORE = None
RAG_DOCUMENT_COUNT = 0
RAG_STARTUP_ERROR = ""

if RAG_AVAILABLE:

    try:

        RAG_STORE, RAG_DOCUMENT_COUNT = get_rag_resources(
            LIVE_SCHEMA_TEXT
        )

    except Exception as rag_startup_exc:

        RAG_STARTUP_ERROR = str(rag_startup_exc)

else:

    RAG_STARTUP_ERROR = RAG_IMPORT_ERROR


with st.sidebar:

    st.divider()

    st.markdown("### 🧠 AI Knowledge")

    if RAG_STORE is not None:

        st.success(
            f"RAG ready · {RAG_DOCUMENT_COUNT} knowledge documents"
        )

    elif RAG_STARTUP_ERROR:

        st.warning(
            "RAG unavailable; live schema prompting is active."
        )

        with st.expander("RAG initialization details"):

            st.code(RAG_STARTUP_ERROR)

    else:

        st.info(
            "Live schema loaded. RAG is not configured."
        )


# ============================================================
# WAREHOUSE OVERVIEW
# ============================================================

total_rows = sum(
    len(frame)
    for frame in frames.values()
)

total_columns = sum(
    len(frame.columns)
    for frame in frames.values()
)

st.markdown(
    '<div class="section-title">📈 Warehouse Overview</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="section-description">'
    'Statistics from your existing Iceberg dimensional warehouse.'
    '</div>',
    unsafe_allow_html=True,
)

metrics = st.columns(4)

metrics[0].metric(
    "Iceberg Tables",
    f"{len(frames):,}",
)

metrics[1].metric(
    "Total Records",
    f"{total_rows:,}",
)

metrics[2].metric(
    "Total Columns",
    f"{total_columns:,}",
)

metrics[3].metric(
    "Namespace",
    str(warehouse_info.get("namespace", "star_schema")),
)


# ============================================================
# TABLE EXPLORER
# ============================================================

with st.expander("🗂️ Explore Warehouse Tables and Schema"):

    table_names = list(frames.keys())

    selected_table = st.selectbox(
        "Select a table",
        table_names,
    )

    selected_frame = frames[selected_table]

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Rows",
        f"{len(selected_frame):,}",
    )

    c2.metric(
        "Columns",
        len(selected_frame.columns),
    )

    c3.metric(
        "Memory Usage",
        f"{selected_frame.memory_usage(deep=True).sum() / 1024**2:.2f} MB",
    )

    st.write("**Available columns**")

    st.code(
        ", ".join(
            str(column)
            for column in selected_frame.columns
        ),
        language=None,
    )

    st.write("**Sample records**")

    st.dataframe(
        selected_frame.head(10),
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# QUESTION INPUT
# ============================================================

st.markdown(
    '<div class="section-title">🤖 Ask Your Retail Data</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="section-description">'
    'Ask one business question and automatically view the results '
    'through four visualizations.'
    '</div>',
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div style="
        background:linear-gradient(110deg,#EFEDFF,#F1F6FF);
        border:1px solid #DFE1FF;
        border-radius:12px;
        padding:14px 17px;
        margin-bottom:15px;
    ">

        <span style="
            font-size:13px;
            color:#514E9C;
            font-weight:700;
        ">
            ✨ AI ANALYTICS ASSISTANT
        </span>

        <div style="
            font-size:12px;
            color:#656F91;
            margin-top:5px;
        ">
            Example: Show sales revenue by region, category and product.
        </div>

    </div>
    """,
    unsafe_allow_html=True,
)

question = st.text_area(
    "Enter your business question",
    key="business_question",
    placeholder="Ask a question about your retail warehouse...",
    height=105,
    label_visibility="collapsed",
)

run_clicked = st.button(
    "✨ Generate Analytics",
    type="primary",
    use_container_width=True,
)


# ============================================================
# RUN ORIGINAL QUERY
# ============================================================

if run_clicked:

    original_question = question.strip()

    if not original_question:

        st.warning(
            "Please enter a business question."
        )

    else:

        st.session_state["dashboard_result"] = None
        st.session_state["dashboard_error"] = None

        st.session_state["dashboard_question"] = (
            original_question
        )

        with st.spinner(
            "Generating SQL and executing your original question..."
        ):

            result = execute_business_question(
                original_question,
                frames,
                connection,
                LIVE_SCHEMA_TEXT,
                RAG_STORE,
            )

        if result.get("error"):

            st.session_state["dashboard_error"] = (
                result["error"]
            )

        else:

            st.session_state["dashboard_result"] = result


# ============================================================
# DISPLAY QUERY ERROR
# ============================================================

if st.session_state.get("dashboard_error"):

    st.error(
        "Analytics generation failed."
    )

    st.code(
        st.session_state["dashboard_error"]
    )


# ============================================================
# ANALYTICS WORKSPACE - FOUR AUTOMATIC VIEWS
# ============================================================

result = st.session_state.get("dashboard_result")

if result is not None:

    result_df = result.get("data")

    analysis = result.get("analysis") or {}

    original_question = st.session_state.get(
        "dashboard_question",
        "",
    )

    st.divider()

    st.markdown(
        '<div class="section-title">📊 Retail Analytics Workspace</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        <div style="
            background:white;
            border:1px solid #E5EAF3;
            padding:15px 18px;
            border-radius:11px;
            margin:12px 0 20px 0;
        ">

            <div style="
                color:#8490A5;
                font-size:11px;
                font-weight:700;
            ">
                ORIGINAL BUSINESS QUESTION
            </div>

            <div style="
                color:#24375B;
                font-size:15px;
                font-weight:650;
                margin-top:6px;
            ">
                {html.escape(original_question)}
            </div>

        </div>
        """,
        unsafe_allow_html=True,
    )

    if result_df is None or result_df.empty:

        st.warning(
            "The original query returned no records."
        )

    else:

        # ----------------------------------------------------
        # RESULT INFORMATION
        # ----------------------------------------------------

        info_cols = st.columns(3)

        info_cols[0].metric(
            "Result Rows",
            f"{len(result_df):,}",
        )

        info_cols[1].metric(
            "Available Columns",
            len(result_df.columns),
        )

        info_cols[2].metric(
            "SQL Executions",
            "1",
        )

        # ----------------------------------------------------
        # AI EXPLANATION
        # ----------------------------------------------------

        if analysis.get("explanation"):

            st.markdown(
                "### 💡 AI Business Insights"
            )

            st.info(
                str(analysis["explanation"])
            )

        # ----------------------------------------------------
        # ORIGINAL SQL
        # ----------------------------------------------------

        with st.expander("🔍 Inspect original SQL"):

            st.code(
                result.get("sql", ""),
                language="sql",
            )

        # ----------------------------------------------------
        # AUTOMATIC DIMENSION AND MEASURE
        # ----------------------------------------------------

        selected_dimension = get_default_dimension(
            result_df
        )

        selected_measure = get_default_measure(
            result_df
        )

        if selected_dimension is None:

            st.warning(
                "The SQL result does not contain a suitable "
                "categorical or date dimension for visualization. "
                "Include a column such as month, region, category, "
                "product or customer city in your SQL result."
            )

        elif selected_measure is None:

            st.warning(
                "The SQL result does not contain a numeric measure "
                "for visualization. Include a numeric column such "
                "as revenue, sales_amount, quantity or total_spend."
            )

        else:

            # ------------------------------------------------
            # KPI SUMMARY
            # ------------------------------------------------

            st.markdown(
                "### 📌 Analysis Summary"
            )

            kpi1, kpi2, kpi3 = st.columns(3)

            kpi1.metric(
                "Analysis Dimension",
                selected_dimension.replace(
                    "_", " "
                ).title(),
            )

            kpi2.metric(
                "Business Measure",
                selected_measure.replace(
                    "_", " "
                ).title(),
            )

            kpi3.metric(
                "Data Points",
                f"{result_df[selected_dimension].nunique():,}",
            )

            st.divider()

            # ------------------------------------------------
            # FOUR AUTOMATIC CHARTS
            # ------------------------------------------------

            show_four_views(
                result_df,
                selected_dimension,
                selected_measure,
            )

        # ----------------------------------------------------
        # ORIGINAL QUERY RESULT
        # ----------------------------------------------------

        st.divider()

        with st.expander("📋 View original query result"):

            st.dataframe(
                result_df,
                use_container_width=True,
                hide_index=True,
            )

            st.download_button(
                "⬇️ Download original query results",
                data=result_df.to_csv(
                    index=False,
                ).encode("utf-8-sig"),
                file_name="retail_query_results.csv",
                mime="text/csv",
                key="download_original_results",
            )


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.markdown(
    """
    <div class="footer-text">

        <b>AI Retail Intelligence Platform</b><br>

        Apache Iceberg · DuckDB · Local Ollama · Streamlit<br>

        Four automatic visualization views using one original SQL execution.
        Source Iceberg tables are never modified.

    </div>
    """,
    unsafe_allow_html=True,
)
