from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException


# ============================================================
# PROJECT CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(r"D:\airflow_etl")

RAW_FOLDER = PROJECT_ROOT / "raw"

PYTHON_EXE = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"


# ============================================================
# EXISTING PROJECT SCRIPTS
# ============================================================

EXTRACT_SCRIPT = (
    PROJECT_ROOT
    / "src"
    / "extract_data_from_raw_pandas.py"
)

ICEBERG_SCRIPT = (
    PROJECT_ROOT
    / "src"
    / "glue_job"
    / "Table_format_generate_by_PYICEBURG.py"
)

SCD_SCRIPT = (
    PROJECT_ROOT
    / "src"
    / "glue_job"
    / "Fact_dim_table_create_with_scd_2.py"
)

ANALYTICS_SCRIPT = (
    PROJECT_ROOT
    / "src"
    / "glue_job"
    / "query_for_anylytics.py"
)


# ============================================================
# LOGGING
# ============================================================

logger = logging.getLogger(__name__)


# ============================================================
# CHECK NEW FILE
# ============================================================

def check_new_file(**context):

    logger.info("=" * 80)
    logger.info("CHECKING RAW FOLDER FOR NEW FILES")
    logger.info("=" * 80)

    if not RAW_FOLDER.exists():

        raise FileNotFoundError(
            f"Raw folder does not exist: {RAW_FOLDER}"
        )

    files = list(RAW_FOLDER.rglob("*.csv"))

    if not files:

        logger.info(
            "No CSV files found in raw folder."
        )

        raise AirflowSkipException(
            "No input files available."
        )

    logger.info(
        "Found %d CSV file(s) in raw folder.",
        len(files)
    )

    for file in files:

        logger.info(
            "Input file: %s | Size: %d bytes",
            file,
            file.stat().st_size
        )

    # Push file information to XCom
    context["ti"].xcom_push(
        key="raw_files",
        value=[str(file) for file in files]
    )

    logger.info("New file check completed.")


# ============================================================
# RUN PYTHON SCRIPT
# ============================================================

def run_script(script_path):

    logger.info("=" * 80)
    logger.info("STARTING SCRIPT")
    logger.info("Script: %s", script_path)
    logger.info("=" * 80)

    # Check Python environment
    if not PYTHON_EXE.exists():

        raise FileNotFoundError(
            f"Python executable not found: {PYTHON_EXE}"
        )

    # Check script
    if not script_path.exists():

        raise FileNotFoundError(
            f"Python script not found: {script_path}"
        )

    logger.info(
        "Python executable: %s",
        PYTHON_EXE
    )

    logger.info(
        "Working directory: %s",
        PROJECT_ROOT
    )

    # Execute Python script
    result = subprocess.run(

        [
            str(PYTHON_EXE),
            str(script_path)
        ],

        cwd=str(PROJECT_ROOT),

        capture_output=True,

        text=True
    )

    # --------------------------------------------------------
    # STDOUT
    # --------------------------------------------------------

    if result.stdout:

        logger.info(
            "SCRIPT OUTPUT:\n%s",
            result.stdout
        )

    # --------------------------------------------------------
    # STDERR
    # --------------------------------------------------------

    if result.stderr:

        logger.warning(
            "SCRIPT ERROR OUTPUT:\n%s",
            result.stderr
        )

    # --------------------------------------------------------
    # CHECK RESULT
    # --------------------------------------------------------

    if result.returncode != 0:

        logger.error(
            "SCRIPT FAILED"
        )

        logger.error(
            "Return Code: %s",
            result.returncode
        )

        raise RuntimeError(
            f"Script failed: {script_path}"
        )

    logger.info("=" * 80)

    logger.info(
        "SCRIPT COMPLETED SUCCESSFULLY"
    )

    logger.info(
        "Script: %s",
        script_path
    )

    logger.info("=" * 80)


# ============================================================
# TASK 1
# EXTRACT RAW DATA
# ============================================================

def run_extract():

    run_script(
        EXTRACT_SCRIPT
    )


# ============================================================
# TASK 2
# ICEBERG TABLE CREATION / LOAD
# ============================================================

def run_iceberg():

    run_script(
        ICEBERG_SCRIPT
    )


# ============================================================
# TASK 3
# FACT / DIMENSION + SCD TYPE 2
# ============================================================

def run_scd():

    run_script(
        SCD_SCRIPT
    )


# ============================================================
# TASK 4
# ANALYTICS
# ============================================================

def run_analytics():

    run_script(
        ANALYTICS_SCRIPT
    )


# ============================================================
# FAILURE CALLBACK
# ============================================================

def failure_callback(context):

    dag_id = context["dag"].dag_id

    task_id = context["task_instance"].task_id

    execution_date = context.get(
        "execution_date"
    )

    logger.error("=" * 80)
    logger.error("PIPELINE FAILURE")
    logger.error("=" * 80)

    logger.error(
        "DAG: %s",
        dag_id
    )

    logger.error(
        "Failed Task: %s",
        task_id
    )

    logger.error(
        "Execution Date: %s",
        execution_date
    )

    logger.error(
        "Please check the Airflow task logs."
    )

    logger.error("=" * 80)


# ============================================================
# DEFAULT ARGUMENTS
# ============================================================

default_args = {

    "owner": "data-engineering",

    "depends_on_past": False,

    # Retry failed task twice
    "retries": 2,

    # Wait 2 minutes before retry
    "retry_delay": timedelta(
        minutes=2
    ),

    # Failure callback
    "on_failure_callback": failure_callback,

}


# ============================================================
# AIRFLOW DAG
# ============================================================

with DAG(

    dag_id="retail_local_end_to_end_pipeline",

    description=(
        "Local retail data pipeline using "
        "existing Python ETL scripts"
    ),

    default_args=default_args,

    start_date=datetime(
        2026,
        10,
        1
    ),

    # Run every hour
    schedule="0 * * * *",

    # Don't run old missed schedules
    catchup=False,

    # Only one pipeline run at a time
    max_active_runs=1,

    tags=[
        "retail",
        "local",
        "etl",
        "iceberg",
        "scd2",
        "analytics",
        "task11"
    ],

) as dag:


    # ========================================================
    # TASK 0
    # CHECK NEW FILE
    # ========================================================

    check_new_file = PythonOperator(

        task_id="check_new_file",

        python_callable=check_new_file

    )


    # ========================================================
    # TASK 1
    # EXTRACT DATA
    # ========================================================

    extract_data = PythonOperator(

        task_id="extract_data",

        python_callable=run_extract

    )


    # ========================================================
    # TASK 2
    # ICEBERG
    # ========================================================

    iceberg_load = PythonOperator(

        task_id="iceberg_load",

        python_callable=run_iceberg

    )


    # ========================================================
    # TASK 3
    # FACT / DIMENSION + SCD TYPE 2
    # ========================================================

    fact_dimension_scd2 = PythonOperator(

        task_id="fact_dimension_scd2",

        python_callable=run_scd

    )


    # ========================================================
    # TASK 4
    # ANALYTICS
    # ========================================================

    analytics = PythonOperator(

        task_id="analytics",

        python_callable=run_analytics

    )


    # ========================================================
    # DEPENDENCIES
    # ========================================================

    (
        check_new_file
        >> extract_data
        >> iceberg_load
        >> fact_dimension_scd2
        >> analytics
    )