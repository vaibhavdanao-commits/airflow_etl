from pathlib import Path
import subprocess
import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------

PROJECT_ROOT = Path(r"D:\airflow_etl")
RAW_FOLDER = PROJECT_ROOT / "raw"

PYTHON_EXE = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
PIPELINE_SCRIPT = PROJECT_ROOT / "dags"/ "local_etl_dag.py"


# ---------------------------------------------------------
# WAIT UNTIL FILE COPY IS COMPLETED
# ---------------------------------------------------------

def wait_until_file_ready(file_path):
    previous_size = -1
    stable_count = 0

    while stable_count < 3:
        try:
            current_size = file_path.stat().st_size

            if current_size == previous_size:
                stable_count += 1
            else:
                stable_count = 0

            previous_size = current_size

        except FileNotFoundError:
            stable_count = 0

        time.sleep(1)


# ---------------------------------------------------------
# TRIGGER EXISTING PIPELINE
# ---------------------------------------------------------

def trigger_pipeline():

    print(f"\nNew CSV detected.")
    print("Triggering existing local_etl_dag.py...\n")

    result = subprocess.run(
        [
            str(PYTHON_EXE),
            str(PIPELINE_SCRIPT)
        ],
        cwd=str(PROJECT_ROOT)
    )

    if result.returncode == 0:
        print("\nPipeline completed successfully.")
    else:
        print(
            f"\nPipeline failed with exit code: "
            f"{result.returncode}"
        )


# ---------------------------------------------------------
# FILE EVENT HANDLER
# ---------------------------------------------------------

class NewFileHandler(FileSystemEventHandler):

    def on_created(self, event):

        if event.is_directory:
            return

        file_path = Path(event.src_path)

        # Only process CSV files
        if file_path.suffix.lower() != ".csv":
            return

        print(f"New file detected: {file_path.name}")

        # Wait until file copy is complete
        wait_until_file_ready(file_path)

        # Trigger existing pipeline
        trigger_pipeline()


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():

    if not RAW_FOLDER.exists():
        raise FileNotFoundError(
            f"Raw folder not found: {RAW_FOLDER}"
        )

    if not PYTHON_EXE.exists():
        raise FileNotFoundError(
            f"Python executable not found: {PYTHON_EXE}"
        )

    if not PIPELINE_SCRIPT.exists():
        raise FileNotFoundError(
            f"Pipeline script not found: {PIPELINE_SCRIPT}"
        )

    event_handler = NewFileHandler()

    observer = Observer()
    observer.schedule(
        event_handler,
        str(RAW_FOLDER),
        recursive=True
    )

    observer.start()

    print("=" * 60)
    print("FILE WATCHER STARTED")
    print("=" * 60)
    print(f"Watching: {RAW_FOLDER}")
    print("Waiting for new CSV files...")
    print("Press CTRL+C to stop.")
    print("=" * 60)

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping file watcher...")
        observer.stop()

    observer.join()


if __name__ == "__main__":
    main()