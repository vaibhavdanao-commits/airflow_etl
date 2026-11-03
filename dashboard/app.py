
from __future__ import annotations

import os
import sys
import json
import re
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
    page_title="AI Retail Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# PROFESSIONAL UI THEME
# ============================================================

st.markdown(
    """
    <style>

    /* --------------------------------------------------------
       GLOBAL
    -------------------------------------------------------- */

    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    .stApp {
        background: #F4F7FB;
        color: #17233C;
    }

    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 3rem;
        max-width: 1500px;
    }

    /* Hide default Streamlit decoration */

    #MainMenu {
        visibility: hidden;
    }

    footer {
        visibility: hidden;
    }

    header[data-testid="stHeader"] {
        background: transparent;
    }

    /* --------------------------------------------------------
       SIDEBAR
    -------------------------------------------------------- */

    section[data-testid="stSidebar"] {
        background: linear-gradient(
            180deg,
            #101B35 0%,
            #17294B 100%
        );
        border-right: 1px solid #263858;
    }

    section[data-testid="stSidebar"] * {
        color: #E8EEFF !important;
    }

    section[data-testid="stSidebar"] .stButton button {
        background: #20345B;
        border: 1px solid #344A70;
        border-radius: 9px;
        text-align: left;
        color: #FFFFFF !important;
        font-size: 13px;
        transition: all 0.2s ease;
    }

    section[data-testid="stSidebar"] .stButton button:hover {
        background: #334F82;
        border-color: #718DE0;
    }

    section[data-testid="stSidebar"] hr {
        border-color: #344765;
    }

    /* --------------------------------------------------------
       HEADER
    -------------------------------------------------------- */

    .main-header {
        background: linear-gradient(
            115deg,
            #172B52 0%,
            #344F9D 55%,
            #6555C7 100%
        );
        padding: 30px 34px;
        border-radius: 18px;
        margin-bottom: 25px;
        box-shadow: 0 8px 24px rgba(39, 57, 115, 0.13);
    }

    .main-header h1 {
        color: #FFFFFF;
        font-size: 30px;
        font-weight: 800;
        margin: 0;
        letter-spacing: -0.8px;
    }

    .main-header p {
        color: #DDE5FF;
        font-size: 14px;
        margin-top: 9px;
        margin-bottom: 0;
    }

    .header-tag {
        display: inline-block;
        background: rgba(255,255,255,0.15);
        border: 1px solid rgba(255,255,255,0.23);
        padding: 5px 12px;
        border-radius: 20px;
        color: #FFFFFF;
        font-size: 11px;
        font-weight: 600;
        margin-bottom: 12px;
    }

    /* --------------------------------------------------------
       SECTION HEADINGS
    -------------------------------------------------------- */

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

    /* --------------------------------------------------------
       KPI CARDS
    -------------------------------------------------------- */

    [data-testid="stMetric"] {
        background: #FFFFFF;
        border: 1px solid #E5EAF3;
        border-radius: 14px;
        padding: 19px 20px;
        box-shadow: 0 3px 12px rgba(29, 47, 83, 0.045);
        min-height: 112px;
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

    /* --------------------------------------------------------
       CARDS AND CONTAINERS
    -------------------------------------------------------- */

    [data-testid="stVerticalBlockBorderWrapper"] {
        background: #FFFFFF;
        border: 1px solid #E5EAF3;
        border-radius: 15px;
        box-shadow: 0 3px 12px rgba(29, 47, 83, 0.035);
    }

    /* --------------------------------------------------------
       BUTTONS
    -------------------------------------------------------- */

    .stButton > button,
    .stDownloadButton > button {
        border-radius: 9px;
        font-weight: 600;
        min-height: 40px;
        transition: all 0.2s ease;
    }

    .stButton > button[kind="primary"] {
        background: linear-gradient(
            110deg,
            #475FD6,
            #7159D9
        );
        color: white;
        border: none;
        box-shadow: 0 4px 12px rgba(83, 91, 205, 0.2);
    }

    .stButton > button[kind="primary"]:hover {
        background: #354DBA;
        border: none;
    }

    /* --------------------------------------------------------
       INPUTS
    -------------------------------------------------------- */

    .stTextArea textarea {
        background: #FFFFFF !important;
        border: 1px solid #DCE3EF !important;
        border-radius: 11px !important;
        color: #17233C !important;
        font-size: 14px !important;
    }

    .stTextArea textarea:focus {
        border-color: #6375DD !important;
        box-shadow: 0 0 0 2px rgba(99,117,221,0.12) !important;
    }

    /* --------------------------------------------------------
       TABS
    -------------------------------------------------------- */

    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
        border-bottom: 1px solid #E2E8F2;
    }

    .stTabs [data-baseweb="tab"] {
        color: #69778E;
        font-weight: 600;
        padding: 10px 15px;
    }

    .stTabs [aria-selected="true"] {
        color: #4E5FD1 !important;
    }

    /* --------------------------------------------------------
       EXPANDERS
    -------------------------------------------------------- */

    [data-testid="stExpander"] {
        border: 1px solid #E5EAF3;
        border-radius: 10px;
        background: #FFFFFF;
    }

    /* --------------------------------------------------------
       DATAFRAME
    -------------------------------------------------------- */

    [data-testid="stDataFrame"] {
        border: 1px solid #E5EAF3;
        border-radius: 10px;
    }

    /* --------------------------------------------------------
       STATUS LABELS
    -------------------------------------------------------- */

    .status-pill {
        display: inline-block;
        padding: 5px 11px;
        border-radius: 20px;
        background: #E1F7EC;
        color: #168653;
        font-size: 11px;
        font-weight: 700;
    }

    .ai-pill {
        display: inline-block;
        padding: 5px 11px;
        border-radius: 20px;
        background: #EFEDFF;
        color: #6653CC;
        font-size: 11px;
        font-weight: 700;
    }

    .question-card {
        background: #FFFFFF;
        border: 1px solid #E4E9F3;
        border-radius: 15px;
        padding: 20px;
        margin-bottom: 15px;
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
            Explore your Iceberg dimensional warehouse using natural
            language. Generate SQL, discover business insights,
            and visualize your retail performance.
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


# ============================================================
# SCHEMA HELPERS
# ============================================================

def find_table(frames, name):

    for table_name, frame in frames.items():

        if table_name.lower() == name.lower():
            return table_name, frame

    return None, None


def get_columns(frame):

    if frame is None:
        return []

    return [str(column) for column in frame.columns]


def find_column(frame, candidates):

    columns = {
        column.lower(): column
        for column in get_columns(frame)
    }

    for candidate in candidates:

        if candidate.lower() in columns:
            return columns[candidate.lower()]

    return None


def get_warehouse_schema(frames):

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
    question,
    frames,
    max_related=4,
):

    schema = get_warehouse_schema(frames)

    prompt = f"""
You are a senior retail analytics business analyst.

Original user question:
{question}

Available warehouse schema:
{schema}

Generate exactly {max_related} useful and DISTINCT related
business questions that can be answered using only the
available tables and columns.

Requirements:
- Stay relevant to the original question.
- Prefer complementary analysis.
- Use only available dimensions and measures.
- Use fact_sales and dimensions only when appropriate.
- Do not assume unavailable columns.
- Preserve important filters from the original question.
- Do not repeat the original question.

Return ONLY valid JSON:
{{"questions": ["question 1", "question 2"]}}
"""

    try:

        import query_engine

        llm_factory = getattr(
            query_engine,
            "make_llm",
            None,
        )

        if llm_factory is None:

            return generate_schema_based_questions(
                question,
                frames,
                max_related,
            )

        llm = llm_factory()

        response = llm.invoke(prompt)

        content = getattr(response, "content", response)

        if isinstance(content, list):

            content = "\n".join(
                str(part.get("text", ""))
                if isinstance(part, dict)
                else str(part)
                for part in content
            )

        text = str(content).strip()

        text = re.sub(
            r"^```(?:json)?\s*",
            "",
            text,
            flags=re.IGNORECASE,
        )

        text = re.sub(r"\s*```$", "", text)

        try:

            payload = json.loads(text)

        except json.JSONDecodeError:

            match = re.search(
                r"\{.*\}",
                text,
                flags=re.DOTALL,
            )

            if not match:
                raise ValueError(
                    "Invalid AI response."
                )

            payload = json.loads(match.group(0))

        questions = payload.get("questions", [])

        results = []

        seen = {question.strip().lower()}

        if not isinstance(questions, list):
            raise ValueError("Invalid question list.")

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
            "Using schema-based suggestions."
        )

    return generate_schema_based_questions(
        question,
        frames,
        max_related,
    )


# ============================================================
# FALLBACK RELATED QUESTIONS
# ============================================================

def generate_schema_based_questions(
    question,
    frames,
    max_related=4,
):

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

        numeric = fact.select_dtypes(
            include="number"
        ).columns.tolist()

        measure = numeric[0] if numeric else None

    if not measure:
        return []

    questions = []

    original = question.strip().lower()

    def add(text):

        if (
            text.strip().lower() != original
            and text not in questions
            and len(questions) < max_related
        ):
            questions.append(text)

    product_name = find_column(
        product,
        ["product_name"],
    )

    category = find_column(
        product,
        ["category"],
    )

    city = find_column(
        customer,
        ["city"],
    )

    segment = find_column(
        customer,
        ["customer_segment"],
    )

    store_name = find_column(
        store,
        ["store_name", "store_id"],
    )

    month = find_column(
        date,
        ["year_month", "month", "month_name", "full_date", "date"],
    )

    q = question.lower()

    if any(word in q for word in ("product", "category", "item")):

        if product is not None and category:

            add(
                f"Show total {measure} by product category. "
                f"Use {fact_table} and {product_table} "
                "with the actual matching product key."
            )

        if product is not None and product_name:

            add(
                f"Show the top 10 products by total {measure}. "
                f"Use {fact_table} and {product_table} "
                "with the actual matching product key."
            )

    if any(word in q for word in ("month", "monthly", "trend", "year", "date")):

        if date is not None and month:

            add(
                f"Show total {measure} by {month} "
                "in chronological order. "
                f"Use {fact_table} and {date_table} "
                "with the actual matching date key."
            )

    if any(word in q for word in ("city", "customer", "region")):

        if customer is not None and city:

            add(
                f"Show total {measure} by customer {city}. "
                f"Use {fact_table} and {customer_table} "
                "with the actual matching customer key."
            )

        if customer is not None and segment:

            add(
                f"Show total {measure} by customer {segment}. "
                f"Use {fact_table} and {customer_table} "
                "with the actual matching customer key."
            )

    if date is not None and month:

        add(
            f"Show total {measure} by {month} "
            "in chronological order. "
            f"Use {fact_table} and {date_table}."
        )

    if product is not None and category:

        add(
            f"Show total {measure} by product category. "
            f"Use {fact_table} and {product_table}."
        )

    if product is not None and product_name:

        add(
            f"Show the top 10 products by total {measure}. "
            f"Use {fact_table} and {product_table}."
        )

    if customer is not None and city:

        add(
            f"Show total {measure} by customer city. "
            f"Use {fact_table} and {customer_table}."
        )

    if customer is not None and segment:

        add(
            f"Show total {measure} by customer segment. "
            f"Use {fact_table} and {customer_table}."
        )

    if store is not None and store_name:

        add(
            f"Show total {measure} by {store_name}. "
            f"Use {fact_table} and {store_table}."
        )

    return questions[:max_related]


# ============================================================
# EXECUTE BUSINESS QUESTION
# ============================================================

def execute_business_question(
    question,
    frames,
    connection,
):

    result = {
        "question": question,
        "analysis": None,
        "sql": "",
        "data": None,
        "error": None,
    }

    try:

        analysis = generate_analysis(
            question,
            frames,
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

        result["data"] = data

    except Exception as exc:

        result["error"] = str(exc)

    return result


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

        st.caption("Warehouse namespace")

        st.code(
            str(namespace),
            language=None,
        )

    except Exception as exc:

        st.warning(
            f"Configuration unavailable: {exc}"
        )

    st.markdown(
        '<span class="status-pill">● Local Warehouse</span>',
        unsafe_allow_html=True,
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
        "🧹 Clear Dashboard Results",
        use_container_width=True,
    ):

        for key in [
            "dashboard_results",
            "dashboard_question",
            "dashboard_error",
            "generated_questions",
        ]:

            st.session_state.pop(key, None)

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

    st.divider()

    st.markdown(
        """
        <div style="font-size:11px;color:#AAB9D6;">
            <b>AI ENGINE</b><br>
            Local Ollama<br><br>
            <b>QUERY ENGINE</b><br>
            DuckDB + SQL<br><br>
            <b>DATA FORMAT</b><br>
            Apache Iceberg
        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# LOAD WAREHOUSE
# ============================================================

try:

    frames, warehouse_info, connection = get_warehouse()

except Exception as exc:

    st.error(
        "Warehouse initialization failed."
    )

    st.code(str(exc))

    st.info(
        """
        Please verify:

        - Catalog database path in dashboard/.env
        - Iceberg warehouse path
        - Namespace configuration (star_schema)
        - PyIceberg and DuckDB dependencies
        - Availability of all required warehouse tables
        """
    )

    st.stop()


frames_by_name = frames

total_rows = sum(
    len(frame)
    for frame in frames_by_name.values()
)

total_columns = sum(
    len(frame.columns)
    for frame in frames_by_name.values()
)


# ============================================================
# WAREHOUSE OVERVIEW
# ============================================================

st.markdown(
    '<div class="section-title">📈 Warehouse Overview</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="section-description">'
    'Live statistics from your existing Iceberg dimensional warehouse.'
    '</div>',
    unsafe_allow_html=True,
)

namespace_name = warehouse_info.get(
    "namespace",
    "star_schema",
)

metric_cols = st.columns(4)

with metric_cols[0]:

    st.metric(
        "Iceberg Tables",
        f"{len(frames_by_name):,}",
        help="Number of loaded Iceberg tables.",
    )

with metric_cols[1]:

    st.metric(
        "Total Records",
        f"{total_rows:,}",
        help="Total rows across loaded tables.",
    )

with metric_cols[2]:

    st.metric(
        "Total Columns",
        f"{total_columns:,}",
        help="Total available columns.",
    )

with metric_cols[3]:

    st.metric(
        "Namespace",
        str(namespace_name),
    )


# ============================================================
# TABLE EXPLORER
# ============================================================

with st.expander("🗂️ Explore Warehouse Tables and Schema"):

    table_names = list(frames_by_name.keys())

    selected_table = st.selectbox(
        "Select a table",
        table_names,
    )

    selected_frame = frames_by_name[selected_table]

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Rows",
        f"{len(selected_frame):,}",
    )

    col2.metric(
        "Columns",
        len(selected_frame.columns),
    )

    col3.metric(
        "Memory Usage",
        f"{selected_frame.memory_usage(deep=True).sum() / 1024**2:.2f} MB",
    )

    st.markdown("**Available columns**")

    st.code(
        ", ".join(
            str(column)
            for column in selected_frame.columns
        ),
        language=None,
    )

    st.markdown("**Sample records**")

    st.dataframe(
        selected_frame.head(10),
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# AI QUESTION INPUT
# ============================================================

st.markdown(
    '<div class="section-title">🤖 Ask Your Retail Data</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="section-description">'
    'Describe what you want to analyze. AI will generate SQL '
    'and create multiple related analytical dashboards.'
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
        <span style="font-size:13px;color:#514E9C;font-weight:700;">
            ✨ AI ANALYTICS ASSISTANT
        </span>
        <div style="font-size:12px;color:#656F91;margin-top:5px;">
            Example: Show monthly revenue for Product_0094
            and explain its performance trend.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

question = st.text_area(
    "Enter your business question",
    key="business_question",
    placeholder=(
        "Ask something like: Show monthly sales revenue "
        "and identify the best performing product categories..."
    ),
    height=105,
    label_visibility="collapsed",
)

col1, col2 = st.columns([1, 4])

with col1:

    run_clicked = st.button(
        "✨ Generate Analytics",
        type="primary",
        use_container_width=True,
    )

with col2:

    st.markdown(
        """
        <div style="
            padding-top:10px;
            font-size:12px;
            color:#8490A5;
        ">
            🔒 Local AI processing &nbsp; • &nbsp;
            Read-only SQL execution &nbsp; • &nbsp;
            Independent analytics
        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# GENERATE MAIN + RELATED DASHBOARDS
# ============================================================

if run_clicked:

    original_question = question.strip()

    if not original_question:

        st.warning(
            "Please enter a business question first."
        )

    else:

        st.session_state.pop(
            "dashboard_results",
            None,
        )

        st.session_state.pop(
            "dashboard_error",
            None,
        )

        try:

            with st.spinner(
                "AI is discovering related business questions..."
            ):

                related = generate_related_questions(
                    original_question,
                    frames_by_name,
                    max_related=4,
                )

            all_questions = [
                original_question
            ] + related

            st.session_state[
                "generated_questions"
            ] = all_questions

            results = []

            progress = st.progress(0.0)

            status = st.empty()

            for index, current_question in enumerate(
                all_questions
            ):

                status.info(
                    f"Analyzing dashboard {index + 1} "
                    f"of {len(all_questions)}: {current_question}"
                )

                item = execute_business_question(
                    current_question,
                    frames_by_name,
                    connection,
                )

                results.append(item)

                progress.progress(
                    (index + 1) / len(all_questions)
                )

            progress.empty()
            status.empty()

            st.session_state[
                "dashboard_results"
            ] = results

            st.session_state[
                "dashboard_question"
            ] = original_question

        except Exception as exc:

            st.session_state[
                "dashboard_error"
            ] = str(exc)


# ============================================================
# DISPLAY ERRORS
# ============================================================

if st.session_state.get("dashboard_error"):

    st.error(
        "Dashboard generation failed."
    )

    st.code(
        st.session_state["dashboard_error"]
    )


# ============================================================
# DISPLAY ANALYTICS DASHBOARDS
# ============================================================

dashboard_results = st.session_state.get(
    "dashboard_results",
    [],
)

dashboard_question = st.session_state.get(
    "dashboard_question",
    "",
)


if dashboard_results:

    successful = [
        item
        for item in dashboard_results
        if item.get("data") is not None
    ]

    failed = [
        item
        for item in dashboard_results
        if item.get("error")
    ]

    st.divider()

    st.markdown(
        '<div class="section-title">'
        '📊 Your Analytics Workspace'
        '</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        <div style="
            background:#FFFFFF;
            border:1px solid #E5EAF3;
            padding:14px 18px;
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
                {dashboard_question}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # --------------------------------------------------------
    # RESULT SUMMARY
    # --------------------------------------------------------

    summary_cols = st.columns(3)

    with summary_cols[0]:

        st.metric(
            "Dashboards Generated",
            len(dashboard_results),
        )

    with summary_cols[1]:

        st.metric(
            "Successful Queries",
            len(successful),
        )

    with summary_cols[2]:

        st.metric(
            "Failed Queries",
            len(failed),
        )

    # --------------------------------------------------------
    # QUERY NAVIGATION
    # --------------------------------------------------------

    st.markdown(
        "#### Generated Business Questions"
    )

    for index, item in enumerate(
        dashboard_results,
        start=1,
    ):

        analysis = item.get("analysis") or {}

        title = (
            analysis.get("title")
            or item.get("question", "")
        )

        if item.get("error"):

            status_label = "⚠️ Failed"

        else:

            status_label = "✓ Completed"

        st.markdown(
            f"""
            <div style="
                background:#FFFFFF;
                border:1px solid #E6EAF2;
                padding:11px 15px;
                border-radius:9px;
                margin-bottom:7px;
            ">
                <span style="
                    color:#4D5FD0;
                    font-weight:750;
                    font-size:13px;
                ">
                    {index:02d}.
                </span>
                <span style="
                    color:#253653;
                    font-weight:600;
                    font-size:13px;
                ">
                    {title}
                </span>
                <span style="
                    float:right;
                    color:#718198;
                    font-size:11px;
                ">
                    {status_label}
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.divider()

    # --------------------------------------------------------
    # INDIVIDUAL DASHBOARD PANELS
    # --------------------------------------------------------

    for index, item in enumerate(
        dashboard_results,
        start=1,
    ):

        analysis = item.get("analysis") or {}

        result_df = item.get("data")

        current_question = item.get(
            "question",
            "",
        )

        title = (
            analysis.get("title")
            or current_question
        )

        with st.container(border=True):

            st.markdown(
                f"""
                <div style="
                    display:flex;
                    align-items:center;
                    gap:10px;
                    margin-bottom:4px;
                ">
                    <div style="
                        background:#EFEDFF;
                        color:#6653CC;
                        width:34px;
                        height:34px;
                        border-radius:9px;
                        display:flex;
                        align-items:center;
                        justify-content:center;
                        font-weight:800;
                        font-size:14px;
                    ">
                        {index:02d}
                    </div>
                    <div>
                        <div style="
                            font-size:16px;
                            color:#1D3154;
                            font-weight:750;
                        ">
                            {title}
                        </div>
                        <div style="
                            font-size:11px;
                            color:#8490A5;
                        ">
                            Independent analytical query
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            st.caption(
                current_question
            )

            if item.get("error"):

                st.error(
                    "This query failed. "
                    "Other dashboard panels remain available."
                )

                with st.expander(
                    "View error details"
                ):

                    st.code(
                        item["error"]
                    )

                if item.get("sql"):

                    with st.expander(
                        "View generated SQL"
                    ):

                        st.code(
                            item["sql"],
                            language="sql",
                        )

                continue

            if result_df is None:

                st.warning(
                    "No result was returned."
                )

                continue

            if analysis.get("explanation"):

                st.info(
                    analysis["explanation"]
                )

            # ------------------------------------------------
            # QUERY INFORMATION
            # ------------------------------------------------

            result_cols = st.columns(3)

            with result_cols[0]:

                st.metric(
                    "Result Rows",
                    f"{len(result_df):,}",
                )

            with result_cols[1]:

                st.metric(
                    "Result Columns",
                    len(result_df.columns),
                )

            with result_cols[2]:

                st.markdown(
                    '<span class="status-pill">'
                    '✓ QUERY SUCCESS'
                    '</span>',
                    unsafe_allow_html=True,
                )

            with st.expander(
                "🔍 Inspect generated SQL"
            ):

                st.code(
                    item.get("sql", ""),
                    language="sql",
                )

            if result_df.empty:

                st.info(
                    "This query returned no rows."
                )

                continue

            # ------------------------------------------------
            # CHART AND DATA TABS
            # ------------------------------------------------

            tab_chart, tab_data = st.tabs(
                [
                    "📈 Visual Analytics",
                    "📋 Detailed Data",
                ]
            )

            with tab_chart:

                st.markdown(
                    "##### Data Visualization"
                )

                show_visualization(
                    result_df,
                    current_question,
                    str(
                        analysis.get(
                            "chart_type",
                            "auto",
                        )
                    ),
                    key_prefix=(
                        f"dashboard_query_{index}"
                    ),
                )

            with tab_data:

                st.markdown(
                    "##### Query Result Records"
                )

                st.dataframe(
                    result_df,
                    use_container_width=True,
                    hide_index=True,
                )

                st.download_button(
                    "⬇️ Download CSV",
                    data=result_df.to_csv(
                        index=False
                    ).encode("utf-8-sig"),
                    file_name=(
                        f"warehouse_query_{index}.csv"
                    ),
                    mime="text/csv",
                    key=(
                        f"download_dashboard_query_{index}"
                    ),
                    use_container_width=True,
                )


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.markdown(
    """
    <div class="footer-text">
        <b>AI Retail Intelligence Platform</b><br>
        Powered by Apache Iceberg · DuckDB · Ollama · Streamlit<br>
        Secure read-only analytics. Source Iceberg tables
        are never modified by dashboard queries.
    </div>
    """,
    unsafe_allow_html=True,
)
