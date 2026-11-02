
"""
============================================================
run.py
Master Execution Script - Retail Data Engineering Project

Execution Sequence:
1. Execute dags/local_etl_dag.py
2. Start Streamlit dashboard/dashboard.py (app.py)
3. Start dags/file_watcher.py in background

Project: D:\\airflow_etl
============================================================
"""

import sys
import subprocess
from pathlib import Path


# ============================================================
# PROJECT CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent

PYTHON_EXE = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"

ETL_SCRIPT = PROJECT_ROOT / "dags" / "local_etl_dag.py"

DASHBOARD_SCRIPT = PROJECT_ROOT / "dashboard" / "app.py"

FILE_WATCHER_SCRIPT = PROJECT_ROOT / "dags" / "file_watcher.py"


# ============================================================
# WINDOWS PROCESS CONFIGURATION
# ============================================================

CREATE_NEW_CONSOLE = subprocess.CREATE_NEW_CONSOLE

dashboard_process = None
watcher_process = None


# ============================================================
# PRINT HEADER
# ============================================================

def print_header():

    print("\n" + "=" * 70)
    print("          RETAIL DATA ENGINEERING PROJECT")
    print("                  MASTER RUNNER")
    print("=" * 70)

    print(f"\nProject Root: {PROJECT_ROOT}")


# ============================================================
# VALIDATE REQUIRED FILES
# ============================================================

def validate_files():

    print("\nValidating project files...")

    required_files = {
        "Python Environment": PYTHON_EXE,
        "ETL Pipeline": ETL_SCRIPT,
        "Streamlit Dashboard": DASHBOARD_SCRIPT,
        "File Watcher": FILE_WATCHER_SCRIPT
    }

    for name, file_path in required_files.items():

        if not file_path.exists():

            raise FileNotFoundError(
                f"{name} not found: {file_path}"
            )

        print(f"[OK] {name}: {file_path}")

    print("\nAll required files validated successfully.")


# ============================================================
# STEP 1: EXECUTE LOCAL ETL PIPELINE
# ============================================================

def run_etl_pipeline():

    print("\n" + "=" * 70)
    print("STEP 1: EXECUTING LOCAL ETL PIPELINE")
    print("=" * 70)

    print(f"\nScript: {ETL_SCRIPT}")

    result = subprocess.run(
        [
            str(PYTHON_EXE),
            str(ETL_SCRIPT)
        ],
        cwd=str(PROJECT_ROOT)
    )

    if result.returncode != 0:

        raise RuntimeError(
            f"ETL pipeline failed with exit code: {result.returncode}"
        )

    print("\n[SUCCESS] ETL pipeline completed successfully.")


# ============================================================
# STEP 2: START STREAMLIT DASHBOARD
# ============================================================

def start_dashboard():

    print("\n" + "=" * 70)
    print("STEP 2: STARTING STREAMLIT DASHBOARD")
    print("=" * 70)

    command = [
        str(PYTHON_EXE),
        "-m",
        "streamlit",
        "run",
        str(DASHBOARD_SCRIPT),
        "--server.headless",
        "true"
    ]

    process = subprocess.Popen(
        command,
        cwd=str(PROJECT_ROOT),
        creationflags=CREATE_NEW_CONSOLE
    )

    print("\n[SUCCESS] Streamlit dashboard started.")

    print("Dashboard URL: http://localhost:8501")
    print(f"Dashboard PID: {process.pid}")

    return process


# ============================================================
# STEP 3: START FILE WATCHER IN BACKGROUND
# ============================================================

def start_file_watcher():

    print("\n" + "=" * 70)
    print("STEP 3: STARTING FILE WATCHER")
    print("=" * 70)

    command = [
        str(PYTHON_EXE),
        str(FILE_WATCHER_SCRIPT)
    ]

    process = subprocess.Popen(
        command,
        cwd=str(PROJECT_ROOT),
        creationflags=CREATE_NEW_CONSOLE
    )

    print("\n[SUCCESS] File watcher started.")

    print(f"Watcher Script: {FILE_WATCHER_SCRIPT}")
    print(f"Watcher PID: {process.pid}")

    return process


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():

    global dashboard_process
    global watcher_process

    print_header()

    try:

        # Validate all scripts and environment
        validate_files()

        # STEP 1: Run ETL pipeline
        run_etl_pipeline()

        # STEP 2: Start dashboard
        dashboard_process = start_dashboard()

        # STEP 3: Start file watcher
        watcher_process = start_file_watcher()

        # Final execution summary
        print("\n" + "=" * 70)
        print("       PROJECT SERVICES STARTED SUCCESSFULLY")
        print("=" * 70)

        print("\nRUNNING SERVICES:")
        print("----------------------------------------")

        print("1. ETL Pipeline  : COMPLETED")
        print("2. Dashboard    : RUNNING")
        print("3. File Watcher : RUNNING")

        print("----------------------------------------")

        print("\nDashboard URL:")
        print("http://localhost:8501")

        print("\nDashboard PID:", dashboard_process.pid)
        print("Watcher PID  :", watcher_process.pid)

        print("\nMaster runner execution completed.")

    except Exception as error:

        print("\n" + "=" * 70)
        print("PROJECT EXECUTION FAILED")
        print("=" * 70)

        print(f"\nError: {error}")

        # Stop any service already started if startup fails
        for process in [watcher_process, dashboard_process]:

            if process is not None and process.poll() is None:

                process.terminate()

        sys.exit(1)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()
