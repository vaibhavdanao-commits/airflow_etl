from pathlib import Path
from datetime import datetime
import subprocess
import logging
import sys
import time



# ============================================================
# PROJECT CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(r"D:\airflow_etl")

RAW_FOLDER = PROJECT_ROOT / "raw"

# Python executable from project virtual environment
PYTHON_EXE = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"


# ============================================================
# EXISTING PROJECT SCRIPTS
# ============================================================

EXTRACT_SCRIPT = (
    PROJECT_ROOT
    / "src"
    / "glue_job"
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
# PIPELINE CONFIGURATION
# ============================================================

MAX_RETRIES = 2
RETRY_DELAY_SECONDS = 120


# ============================================================
# LOGGING CONFIGURATION
# ============================================================

LOG_FOLDER = PROJECT_ROOT / "logs"
LOG_FOLDER.mkdir(parents=True, exist_ok=True)

LOG_FILE = (
    LOG_FOLDER
    / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


# ============================================================
# CHECK PROJECT CONFIGURATION
# ============================================================

def validate_configuration():

    logger.info("=" * 80)
    logger.info("VALIDATING PROJECT CONFIGURATION")
    logger.info("=" * 80)

    if not PROJECT_ROOT.exists():
        raise FileNotFoundError(
            f"Project root does not exist: {PROJECT_ROOT}"
        )

    if not PYTHON_EXE.exists():
        raise FileNotFoundError(
            f"Python executable not found: {PYTHON_EXE}"
        )

    logger.info(
        "Project root: %s",
        PROJECT_ROOT
    )

    logger.info(
        "Python executable: %s",
        PYTHON_EXE
    )

    logger.info(
        "Raw folder: %s",
        RAW_FOLDER
    )

    logger.info("Configuration validation completed.")


# ============================================================
# CHECK NEW FILE
# ============================================================

def check_new_file():

    logger.info("=" * 80)
    logger.info("CHECKING RAW FOLDER FOR CSV FILES")
    logger.info("=" * 80)

    if not RAW_FOLDER.exists():
        raise FileNotFoundError(
            f"Raw folder does not exist: {RAW_FOLDER}"
        )

    files = list(RAW_FOLDER.rglob("*.csv"))

    if not files:

        logger.warning(
            "No CSV files found in raw folder."
        )

        return False

    logger.info(
        "Found %d CSV file(s).",
        len(files)
    )

    for file in files:

        logger.info(
            "Input file: %s | Size: %d bytes",
            file,
            file.stat().st_size
        )

    logger.info("Input file check completed.")

    return True


# ============================================================
# RUN PYTHON SCRIPT
# ============================================================

def run_script(script_path, task_name):

    logger.info("")
    logger.info("=" * 80)
    logger.info("STARTING TASK: %s", task_name)
    logger.info("=" * 80)

    if not PYTHON_EXE.exists():

        raise FileNotFoundError(
            f"Python executable not found: {PYTHON_EXE}"
        )

    if not script_path.exists():

        raise FileNotFoundError(
            f"Python script not found: {script_path}"
        )

    logger.info(
        "Script: %s",
        script_path
    )

    logger.info(
        "Python: %s",
        PYTHON_EXE
    )

    logger.info(
        "Working directory: %s",
        PROJECT_ROOT
    )

    # --------------------------------------------------------
    # RETRY LOOP
    # --------------------------------------------------------

    for attempt in range(1, MAX_RETRIES + 2):

        logger.info(
            "Execution attempt %d/%d",
            attempt,
            MAX_RETRIES + 1
        )

        try:

            result = subprocess.run(
                [
                    str(PYTHON_EXE),
                    str(script_path),
                ],
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )

            # ------------------------------------------------
            # STDOUT
            # ------------------------------------------------

            if result.stdout:

                logger.info(
                    "SCRIPT OUTPUT:\n%s",
                    result.stdout
                )

            # ------------------------------------------------
            # STDERR
            # ------------------------------------------------

            if result.stderr:

                logger.warning(
                    "SCRIPT STDERR:\n%s",
                    result.stderr
                )

            # ------------------------------------------------
            # SUCCESS
            # ------------------------------------------------

            if result.returncode == 0:

                logger.info("=" * 80)
                logger.info(
                    "TASK COMPLETED SUCCESSFULLY: %s",
                    task_name
                )
                logger.info("=" * 80)

                return True

            # ------------------------------------------------
            # FAILURE
            # ------------------------------------------------

            logger.error(
                "TASK FAILED: %s",
                task_name
            )

            logger.error(
                "Return code: %s",
                result.returncode
            )

            if attempt <= MAX_RETRIES:

                logger.warning(
                    "Retrying task after %d seconds...",
                    RETRY_DELAY_SECONDS
                )

                time.sleep(RETRY_DELAY_SECONDS)

            else:

                raise RuntimeError(
                    f"Task failed after {MAX_RETRIES + 1} attempts: "
                    f"{task_name}"
                )

        except Exception as exc:

            logger.exception(
                "Exception while executing task: %s",
                task_name
            )

            if attempt <= MAX_RETRIES:

                logger.warning(
                    "Retrying task after %d seconds...",
                    RETRY_DELAY_SECONDS
                )

                time.sleep(RETRY_DELAY_SECONDS)

            else:

                raise RuntimeError(
                    f"Task failed: {task_name}"
                ) from exc

    return False


# ============================================================
# EXTRACT RAW DATA
# ============================================================

def run_extract():

    return run_script(
        EXTRACT_SCRIPT,
        "EXTRACT RAW DATA"
    )


# ============================================================
# ICEBERG TABLE CREATION / LOAD
# ============================================================

def run_iceberg():

    return run_script(
        ICEBERG_SCRIPT,
        "ICEBERG TABLE CREATION / LOAD"
    )


# ============================================================
# FACT / DIMENSION + SCD TYPE 2
# ============================================================

def run_scd():

    return run_script(
        SCD_SCRIPT,
        "FACT / DIMENSION + SCD TYPE 2"
    )


# ============================================================
# ANALYTICS
# ============================================================

def run_analytics():

    return run_script(
        ANALYTICS_SCRIPT,
        "ANALYTICS"
    )


# ============================================================
# PIPELINE FAILURE HANDLER
# ============================================================

def pipeline_failure(task_name, error):

    logger.error("")
    logger.error("=" * 80)
    logger.error("PIPELINE FAILURE")
    logger.error("=" * 80)

    logger.error(
        "Failed Task: %s",
        task_name
    )

    logger.error(
        "Error: %s",
        error
    )

    logger.error(
        "Log file: %s",
        LOG_FILE
    )

    logger.error("=" * 80)


# ============================================================
# MAIN PIPELINE
# ============================================================

def main():

    start_time = datetime.now()

    logger.info("")
    logger.info("#" * 80)
    logger.info("LOCAL RETAIL DATA PIPELINE STARTED")
    logger.info("#" * 80)

    logger.info(
        "Start time: %s",
        start_time
    )

    try:

        # ----------------------------------------------------
        # STEP 0
        # CHECK CONFIGURATION
        # ----------------------------------------------------

        validate_configuration()

        # ----------------------------------------------------
        # STEP 1
        # CHECK RAW FILE
        # ----------------------------------------------------

        has_files = check_new_file()

        if not has_files:

            logger.warning(
                "Pipeline stopped because no CSV files are available."
            )

            return 0

        # ----------------------------------------------------
        # STEP 2
        # EXTRACT
        # ----------------------------------------------------

        run_extract()

        # ----------------------------------------------------
        # STEP 3
        # ICEBERG
        # ----------------------------------------------------

        run_iceberg()

        # ----------------------------------------------------
        # STEP 4
        # SCD TYPE 2
        # ----------------------------------------------------

        run_scd()

        # ----------------------------------------------------
        # STEP 5
        # ANALYTICS
        # ----------------------------------------------------

        run_analytics()

        # ----------------------------------------------------
        # PIPELINE SUCCESS
        # ----------------------------------------------------

        end_time = datetime.now()

        duration = end_time - start_time

        logger.info("")
        logger.info("#" * 80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("#" * 80)

        logger.info(
            "Start time: %s",
            start_time
        )

        logger.info(
            "End time: %s",
            end_time
        )

        logger.info(
            "Total duration: %s",
            duration
        )

        logger.info(
            "Log file: %s",
            LOG_FILE
        )

        logger.info("#" * 80)

        return 0

    except Exception as exc:

        pipeline_failure(
            "End-to-End Pipeline",
            exc
        )

        return 1


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    exit_code = main()

    sys.exit(exit_code)
