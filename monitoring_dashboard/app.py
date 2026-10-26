from __future__ import annotations

import sqlite3
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

# ============================================================
# TASK 15 + TASK 16 — DATA LINEAGE, METADATA & MONITORING
# Streamlit dashboard for a local Python/Airflow/AWS-style ETL project.
#
# This dashboard stores monitoring metadata in SQLite. It does NOT
# create, overwrite, append to, or otherwise modify your PyIceberg tables.
# ============================================================

st.set_page_config(
    page_title="Pipeline Lineage & Monitoring",
    page_icon="📊",
    layout="wide",
)

APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent.parent.parent
DEFAULT_DB = PROJECT_ROOT / "Warehouse" / "pipeline_monitoring.db"
DEFAULT_RAW = PROJECT_ROOT / "data" / "raw"
PIPELINE_VERSION = "1.0.0"

FIELDS = [
    "file_name",
    "source",
    "ingestion_time",
    "record_count",
    "valid_count",
    "rejected_count",
    "processing_status",
    "processing_time",
    "pipeline_version",
    "error_count",
    "data_quality_failures",
    "job_name",
    "error_message",
]

def get_db_path() -> Path:
    configured = st.session_state.get("db_path", str(DEFAULT_DB))
    path = Path(configured).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path

def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path(), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def initialize_db() -> None:
    with connect_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT NOT NULL,
                source TEXT NOT NULL,
                ingestion_time TEXT NOT NULL,
                record_count INTEGER NOT NULL DEFAULT 0,
                valid_count INTEGER NOT NULL DEFAULT 0,
                rejected_count INTEGER NOT NULL DEFAULT 0,
                processing_status TEXT NOT NULL,
                processing_time REAL NOT NULL DEFAULT 0,
                pipeline_version TEXT NOT NULL,
                error_count INTEGER NOT NULL DEFAULT 0,
                data_quality_failures INTEGER NOT NULL DEFAULT 0,
                job_name TEXT NOT NULL DEFAULT 'retail-generic-etl',
                error_message TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER,
                event_time TEXT NOT NULL,
                severity TEXT NOT NULL,
                event_type TEXT NOT NULL,
                message TEXT NOT NULL
            )
        """)

def add_event(conn: sqlite3.Connection, run_id: int | None,
              severity: str, event_type: str, message: str) -> None:
    conn.execute(
        """INSERT INTO pipeline_events
           (run_id, event_time, severity, event_type, message)
           VALUES (?, ?, ?, ?, ?)""",
        (run_id, datetime.now().isoformat(timespec="seconds"),
         severity, event_type, message),
    )

def load_runs() -> pd.DataFrame:
    with connect_db() as conn:
        df = pd.read_sql_query(
            "SELECT * FROM pipeline_runs ORDER BY ingestion_time DESC, run_id DESC",
            conn,
        )
    if not df.empty:
        for col in ("ingestion_time", "created_at"):
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

def load_events() -> pd.DataFrame:
    with connect_db() as conn:
        df = pd.read_sql_query(
            "SELECT * FROM pipeline_events ORDER BY event_time DESC, event_id DESC",
            conn,
        )
    if not df.empty:
        df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
    return df

def record_run(*, file_name: str, source: str, record_count: int,
               valid_count: int, rejected_count: int, status: str,
               processing_time: float, error_count: int = 0,
               dq_failures: int = 0, job_name: str = "retail-generic-etl",
               error_message: str = "", pipeline_version: str = PIPELINE_VERSION) -> int:
    now = datetime.now().isoformat(timespec="seconds")
    with connect_db() as conn:
        cur = conn.execute("""
            INSERT INTO pipeline_runs (
                file_name, source, ingestion_time, record_count, valid_count,
                rejected_count, processing_status, processing_time,
                pipeline_version, error_count, data_quality_failures,
                job_name, error_message, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            file_name, source, now, int(record_count), int(valid_count),
            int(rejected_count), status, round(float(processing_time), 3),
            pipeline_version, int(error_count), int(dq_failures),
            job_name, error_message, now,
        ))
        run_id = int(cur.lastrowid)
        severity = "ERROR" if status == "FAILED" else ("WARNING" if rejected_count else "INFO")
        add_event(conn, run_id, severity, "PIPELINE_RUN",
                  f"{job_name}: {status}; records={record_count}; rejected={rejected_count}")
        if error_message:
            add_event(conn, run_id, "ERROR", "PIPELINE_ERROR", error_message[:2000])
        if dq_failures:
            add_event(conn, run_id, "WARNING", "DATA_QUALITY",
                      f"{dq_failures} data-quality failure(s) recorded.")
        return run_id

def inspect_csv(csv_file: Any) -> tuple[int, int, int, int, str]:
    """Basic generic checks. Returns total, valid, rejected, DQ failures, details."""
    df = pd.read_csv(csv_file)
    total = len(df)
    if total == 0:
        return 0, 0, 0, 0, "CSV contains no data rows."

    # Generic checks that do not assume every source has the same schema:
    # blank entire rows and duplicate rows are counted as quality failures.
    blank_rows = int(df.isna().all(axis=1).sum())
    duplicate_rows = int(df.duplicated().sum())
    dq_failures = blank_rows + duplicate_rows
    rejected = min(total, dq_failures)
    valid = total - rejected
    details = f"Blank rows={blank_rows}; duplicate rows={duplicate_rows}."
    return total, valid, rejected, dq_failures, details

def status_color(status: str) -> str:
    return {"SUCCESS": "green", "PARTIAL": "orange", "FAILED": "red"}.get(status, "gray")

initialize_db()

st.title("📊 Data Lineage & Pipeline Monitoring")
st.caption("Task 15 — Metadata and lineage  |  Task 16 — Pipeline health, duration, failures and data quality")
st.info(
    "Monitoring records are stored in a local SQLite database. "
    "This dashboard is read-only with respect to your PyIceberg dimensional warehouse."
)

with st.sidebar:
    st.header("Settings")
    st.text_input("Monitoring SQLite DB path", value=str(DEFAULT_DB), key="db_path")
    st.text_input("Pipeline version", value=PIPELINE_VERSION, key="pipeline_version")
    st.text_input("Default raw CSV folder", value=str(DEFAULT_RAW), key="raw_folder")
    st.divider()
    st.caption("The DB path is applied to subsequent operations. Use a persistent disk for production.")

runs = load_runs()
events = load_events()

# Sidebar filters
with st.sidebar:
    st.header("Filters")
    if not runs.empty:
        statuses = sorted(runs["processing_status"].dropna().unique().tolist())
        selected_statuses = st.multiselect("Job status", statuses, default=statuses)
        jobs = sorted(runs["job_name"].dropna().unique().tolist())
        selected_jobs = st.multiselect("Pipeline/job", jobs, default=jobs)
        days = st.selectbox("Time range", ["All time", "Last 7 days", "Last 30 days", "Last 90 days"])
    else:
        selected_statuses, selected_jobs, days = [], [], "All time"

filtered = runs.copy()
if not filtered.empty:
    if selected_statuses:
        filtered = filtered[filtered["processing_status"].isin(selected_statuses)]
    else:
        filtered = filtered.iloc[0:0]
    if selected_jobs:
        filtered = filtered[filtered["job_name"].isin(selected_jobs)]
    else:
        filtered = filtered.iloc[0:0]
    if days != "All time":
        day_count = int(days.split()[1])
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=day_count)
        filtered = filtered[filtered["ingestion_time"] >= cutoff]

# KPI cards
total_runs = len(filtered)
success_runs = int((filtered["processing_status"] == "SUCCESS").sum()) if total_runs else 0
failed_runs = int((filtered["processing_status"] == "FAILED").sum()) if total_runs else 0
total_records = int(filtered["record_count"].sum()) if total_runs else 0
failed_records = int(filtered["rejected_count"].sum()) if total_runs else 0
avg_duration = float(filtered["processing_time"].mean()) if total_runs else 0.0
success_rate = (success_runs / total_runs * 100) if total_runs else 0.0

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Pipeline Runs", f"{total_runs:,}")
c2.metric("Success Rate", f"{success_rate:.1f}%")
c3.metric("Failed Jobs", f"{failed_runs:,}")
c4.metric("Records Processed", f"{total_records:,}")
c5.metric("Rejected Records", f"{failed_records:,}")
c6.metric("Avg Duration", f"{avg_duration:.2f}s")

tab_overview, tab_lineage, tab_register, tab_events, tab_aws = st.tabs([
    "Overview", "Lineage & Metadata", "Register / Test Run", "Events & Failures", "AWS Production Plan"
])

with tab_overview:
    st.subheader("Pipeline health")
    if filtered.empty:
        st.warning("No monitoring runs match the current filters. Register a test run or process a CSV below.")
    else:
        left, right = st.columns(2)
        with left:
            status_counts = filtered["processing_status"].value_counts().rename_axis("status").reset_index(name="runs")
            st.plotly_chart(px.pie(status_counts, names="status", values="runs",
                                   title="Run status distribution", hole=0.45), use_container_width=True)
        with right:
            daily = filtered.assign(run_date=filtered["ingestion_time"].dt.date).groupby(
                ["run_date", "processing_status"], dropna=False
            ).size().reset_index(name="runs")
            st.plotly_chart(px.bar(daily, x="run_date", y="runs", color="processing_status",
                                   title="Runs over time", barmode="stack"), use_container_width=True)

        left, right = st.columns(2)
        with left:
            st.plotly_chart(px.bar(filtered.sort_values("ingestion_time"), x="ingestion_time",
                                   y="record_count", color="processing_status",
                                   hover_data=["file_name", "job_name"],
                                   title="Records processed by run"), use_container_width=True)
        with right:
            st.plotly_chart(px.scatter(filtered, x="processing_time", y="rejected_count",
                                       color="processing_status", size="record_count",
                                       hover_data=["file_name", "job_name"],
                                       title="Duration vs rejected records"), use_container_width=True)

        st.subheader("Latest pipeline runs")
        show_cols = ["run_id", "ingestion_time", "job_name", "file_name", "record_count",
                     "valid_count", "rejected_count", "processing_status", "processing_time",
                     "error_count", "data_quality_failures", "pipeline_version"]
        st.dataframe(filtered[show_cols].head(25), use_container_width=True, hide_index=True)

with tab_lineage:
    st.subheader("Ingestion metadata and lineage")
    st.markdown("Each row links a source file to the job execution, counts, status, version and error details.")
    if filtered.empty:
        st.info("No lineage metadata available for the selected filters.")
    else:
        cols = ["run_id"] + FIELDS
        st.dataframe(filtered[[c for c in cols if c in filtered.columns]],
                     use_container_width=True, hide_index=True)
        st.download_button(
            "Download lineage metadata (CSV)",
            data=filtered[[c for c in cols if c in filtered.columns]].to_csv(index=False).encode("utf-8"),
            file_name="pipeline_lineage_metadata.csv",
            mime="text/csv",
        )
    st.markdown("**Example interpretation**")
    st.code(
        "File             : orders_20260910.csv\n"
        "Records          : 125,430\n"
        "Valid            : 124,982\n"
        "Rejected         : 448\n"
        "Status           : SUCCESS\n"
        "Processed At     : 2026-09-10 03:45\n"
        "Pipeline Version : 1.0.0",
        language="yaml",
    )

with tab_register:
    st.subheader("Register a pipeline run")
    st.caption("You can record a real run from another script, or use the form to test the dashboard.")
    raw_folder = Path(st.session_state.get("raw_folder", str(DEFAULT_RAW))).expanduser()
    csv_files = sorted(raw_folder.glob("*.csv")) if raw_folder.exists() else []
    with st.expander("Process and register a CSV file", expanded=True):
        uploaded = st.file_uploader("Choose a CSV file", type=["csv"], key="csv_upload")
        if uploaded is not None:
            st.write(f"Selected file: `{uploaded.name}`")
            source_name = st.text_input("Source system", value="local_csv", key="csv_source")
            job_name = st.text_input("Job name", value="retail-generic-etl", key="csv_job")
            if st.button("Validate CSV and record run", type="primary"):
                started = time.perf_counter()
                try:
                    total, valid, rejected, dq, details = inspect_csv(uploaded)
                    duration = time.perf_counter() - started
                    status = "SUCCESS" if rejected == 0 else ("PARTIAL" if valid > 0 else "FAILED")
                    run_id = record_run(
                        file_name=uploaded.name, source=source_name, record_count=total,
                        valid_count=valid, rejected_count=rejected, status=status,
                        processing_time=duration, error_count=0, dq_failures=dq,
                        job_name=job_name, error_message=details if rejected else "",
                        pipeline_version=st.session_state.get("pipeline_version", PIPELINE_VERSION),
                    )
                    st.success(f"Recorded run #{run_id}: {status}. {details}")
                    st.rerun()
                except Exception as exc:
                    duration = time.perf_counter() - started
                    run_id = record_run(
                        file_name=uploaded.name, source=source_name, record_count=0,
                        valid_count=0, rejected_count=0, status="FAILED",
                        processing_time=duration, error_count=1, dq_failures=0,
                        job_name=job_name, error_message=str(exc),
                        pipeline_version=st.session_state.get("pipeline_version", PIPELINE_VERSION),
                    )
                    st.error(f"CSV processing failed. Recorded run #{run_id}: {exc}")

    with st.expander("Manually enter a run (demo/testing)"):
        with st.form("manual_run_form"):
            col1, col2 = st.columns(2)
            with col1:
                m_file = st.text_input("File name", "orders_20260910.csv")
                m_source = st.text_input("Source", "local_csv")
                m_job = st.text_input("Job name", "retail-generic-etl")
                m_status = st.selectbox("Status", ["SUCCESS", "PARTIAL", "FAILED"])
                m_version = st.text_input("Pipeline version", st.session_state.get("pipeline_version", PIPELINE_VERSION))
            with col2:
                m_total = st.number_input("Records", min_value=0, value=125430, step=100)
                m_valid = st.number_input("Valid records", min_value=0, value=124982, step=100)
                m_rejected = st.number_input("Rejected records", min_value=0, value=448, step=1)
                m_duration = st.number_input("Processing time (seconds)", min_value=0.0, value=12.5, step=0.5)
                m_errors = st.number_input("Error count", min_value=0, value=0, step=1)
                m_dq = st.number_input("Data-quality failures", min_value=0, value=448, step=1)
            m_error = st.text_area("Error details (optional)")
            submitted = st.form_submit_button("Save monitoring record", type="primary")
            if submitted:
                if m_valid + m_rejected > m_total:
                    st.error("Valid + rejected records cannot exceed total records.")
                else:
                    rid = record_run(
                        file_name=m_file, source=m_source, record_count=m_total,
                        valid_count=m_valid, rejected_count=m_rejected, status=m_status,
                        processing_time=m_duration, error_count=m_errors,
                        dq_failures=m_dq, job_name=m_job, error_message=m_error,
                        pipeline_version=m_version,
                    )
                    st.success(f"Saved monitoring record #{rid}.")
                    st.rerun()

    st.subheader("Integration helper for your ETL scripts")
    st.markdown("Import `record_run` from this dashboard module only if your project layout supports importing it. For production, move the function into a separate `monitoring.py` module.")
    st.code(
        "run_id = record_run(\n"
        "    file_name=source_file.name,\n"
        "    source='orders_s3_or_local',\n"
        "    record_count=total_rows,\n"
        "    valid_count=valid_rows,\n"
        "    rejected_count=rejected_rows,\n"
        "    status='SUCCESS' if rejected_rows == 0 else 'PARTIAL',\n"
        "    processing_time=elapsed_seconds,\n"
        "    error_count=error_count,\n"
        "    dq_failures=dq_failure_count,\n"
        "    job_name='retail-generic-etl',\n"
        "    error_message=error_summary,\n"
        ")",
        language="python",
    )

with tab_events:
    st.subheader("Events, data-quality alerts and failures")
    if events.empty:
        st.info("No events recorded yet.")
    else:
        sev = st.multiselect("Severity", ["ERROR", "WARNING", "INFO"], default=["ERROR", "WARNING", "INFO"])
        view_events = events[events["severity"].isin(sev)]
        st.dataframe(view_events, use_container_width=True, hide_index=True)
        st.download_button("Download events (CSV)", view_events.to_csv(index=False).encode("utf-8"),
                           "pipeline_events.csv", "text/csv")
        errors = view_events[view_events["severity"] == "ERROR"]
        if not errors.empty:
            st.error(f"{len(errors)} error event(s) in the current event view.")

with tab_aws:
    st.subheader("How to productionize monitoring on AWS")
    st.markdown("""
| Need | AWS service | Implementation |
|---|---|---|
| Job status and execution duration | **AWS Glue job metrics / CloudWatch** | Emit job run state, start/end time, duration and retry metrics |
| Central logs | **Amazon CloudWatch Logs** | Structured JSON logs with `run_id`, source file, pipeline version and error details |
| Alerts | **Amazon CloudWatch Alarms + SNS** | Alert on failed jobs, high error counts, long duration or missing expected runs |
| Orchestration and retries | **Amazon MWAA (Airflow) or AWS Step Functions** | Track task state, retries, dependencies and recovery |
| File arrival and event triggers | **Amazon S3 Events + EventBridge** | Start ingestion when a source object arrives; route events to a target |
| Metadata / lineage store | **Amazon DynamoDB, RDS, or S3/Athena** | Persist one metadata record per run and query history |
| Data-quality checks | **AWS Glue Data Quality** | Run rules; capture passed/failed rules and rejected record counts |
| Catalog and table discovery | **AWS Glue Data Catalog** | Catalog supported datasets and schemas; pair with explicit run-level lineage metadata |
| Dashboards | **CloudWatch Dashboards or QuickSight** | Show success rate, duration, throughput, rejected rows and failure trends |
""")
    st.markdown("**Suggested production flow**")
    st.code(
        "S3 source file → EventBridge/S3 event → MWAA or Step Functions\n"
        "→ Glue / Python ETL → data-quality checks → curated target\n"
        "→ metadata record + structured CloudWatch logs\n"
        "→ CloudWatch metrics/alarms → SNS email or operations channel",
        language="text",
    )
    st.warning("The local SQLite dashboard is a learning/demo implementation. For multi-user production, use a managed metadata store, IAM least privilege, encryption, retention policies, and alert routing.")

st.divider()
st.caption(
    f"Pipeline metadata dashboard • schema version {PIPELINE_VERSION} • "
    f"Monitoring DB: {get_db_path()}"
)
