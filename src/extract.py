"""
extract.py

Responsible for:
1. Reading source CSV data
2. Validating source availability
3. Counting source records
4. Writing extracted data to staging
5. Returning extraction metadata

Can be executed:
    python src/extract.py

Can also be imported and called from an Airflow DAG.
"""

from pathlib import Path
from datetime import datetime
import csv
import shutil
import sys


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SOURCE_FILE = PROJECT_ROOT / "data" / "source" / "employees.csv"
STAGING_DIR = PROJECT_ROOT / "data" / "staging"

STAGING_FILE = STAGING_DIR / "employees_extracted.csv"


# Expected columns in the source file
EXPECTED_COLUMNS = [
    "employee_id",
    "name",
    "department",
    "salary",
    "status",
]


# ============================================================
# LOGGING
# ============================================================

def log(message: str) -> None:
    """
    Simple console logger.

    Later this can be replaced or extended with
    Python logging / Airflow logging.
    """

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"[{timestamp}] {message}")


# ============================================================
# SOURCE VALIDATION
# ============================================================

def validate_source_file(source_file: Path) -> None:
    """
    Validate that the source file exists and is a CSV file.
    """

    log(f"Checking source file: {source_file}")

    if not source_file.exists():
        raise FileNotFoundError(
            f"Source file does not exist: {source_file}"
        )

    if not source_file.is_file():
        raise ValueError(
            f"Source path is not a file: {source_file}"
        )

    if source_file.suffix.lower() != ".csv":
        raise ValueError(
            f"Unsupported file type: {source_file.suffix}. "
            f"Expected .csv"
        )

    log("Source file validation successful.")


# ============================================================
# READ CSV
# ============================================================

def read_source(source_file: Path) -> list[dict]:
    """
    Read records from the source CSV.

    Returns:
        list[dict]: List of source records.
    """

    log("Reading source data...")

    with source_file.open(
        mode="r",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        reader = csv.DictReader(file)

        if reader.fieldnames is None:
            raise ValueError(
                "Source CSV does not contain a header."
            )

        actual_columns = [
            column.strip()
            for column in reader.fieldnames
        ]

        log(f"Source columns: {actual_columns}")

        validate_schema(actual_columns)

        records = []

        for row_number, row in enumerate(reader, start=2):

            cleaned_row = {
                key.strip() if key else key:
                value.strip() if isinstance(value, str) else value
                for key, value in row.items()
            }

            records.append(cleaned_row)

    log(f"Source records extracted: {len(records)}")

    return records


# ============================================================
# SCHEMA VALIDATION
# ============================================================

def validate_schema(actual_columns: list[str]) -> None:
    """
    Validate that all expected columns exist.
    """

    missing_columns = [
        column
        for column in EXPECTED_COLUMNS
        if column not in actual_columns
    ]

    if missing_columns:

        raise ValueError(
            "Source schema validation failed. "
            f"Missing columns: {missing_columns}"
        )

    log("Source schema validation successful.")


# ============================================================
# RECORD COUNT
# ============================================================

def get_record_count(records: list[dict]) -> int:
    """
    Return number of data records.
    """

    count = len(records)

    log(f"Source record count: {count}")

    return count


# ============================================================
# WRITE STAGING DATA
# ============================================================

def write_to_staging(
    records: list[dict],
    staging_file: Path
) -> None:
    """
    Write extracted records to staging CSV.
    """

    log("Preparing staging directory...")

    staging_file.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    if not records:
        log("Source contains zero records.")

        # Still create an empty CSV with headers
        with staging_file.open(
            mode="w",
            encoding="utf-8",
            newline=""
        ) as file:

            writer = csv.DictWriter(
                file,
                fieldnames=EXPECTED_COLUMNS
            )

            writer.writeheader()

        return

    columns = list(records[0].keys())

    with staging_file.open(
        mode="w",
        encoding="utf-8",
        newline=""
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=columns
        )

        writer.writeheader()
        writer.writerows(records)

    log(
        f"Extracted data written to staging: "
        f"{staging_file}"
    )


# ============================================================
# EXTRACTION FUNCTION
# ============================================================

def extract_data(
    source_file: Path = SOURCE_FILE,
    staging_file: Path = STAGING_FILE,
) -> dict:
    """
    Main extraction function.

    This is the function that will later be called
    from the Airflow DAG.

    Returns:
        dict containing extraction metadata.
    """

    start_time = datetime.now()

    log("=" * 60)
    log("STARTING EXTRACTION")
    log("=" * 60)

    try:

        # ----------------------------------------------------
        # Step 1: Validate source
        # ----------------------------------------------------

        validate_source_file(source_file)

        # ----------------------------------------------------
        # Step 2: Read source
        # ----------------------------------------------------

        records = read_source(source_file)

        # ----------------------------------------------------
        # Step 3: Count records
        # ----------------------------------------------------

        source_count = get_record_count(records)

        # ----------------------------------------------------
        # Step 4: Write staging
        # ----------------------------------------------------

        write_to_staging(
            records,
            staging_file
        )

        # ----------------------------------------------------
        # Step 5: Calculate duration
        # ----------------------------------------------------

        end_time = datetime.now()

        duration_seconds = (
            end_time - start_time
        ).total_seconds()

        result = {
            "status": "SUCCESS",
            "source_file": str(source_file),
            "staging_file": str(staging_file),
            "source_count": source_count,
            "output_count": source_count,
            "difference_count": 0,
            "difference_percentage": 0.0,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration_seconds,
        }

        log("=" * 60)
        log("EXTRACTION SUCCESSFUL")
        log(f"Source Count : {source_count}")
        log(f"Output Count : {source_count}")
        log(f"Difference   : 0")
        log(f"Duration     : {duration_seconds:.2f} seconds")
        log("=" * 60)

        return result

    except Exception as error:

        end_time = datetime.now()

        duration_seconds = (
            end_time - start_time
        ).total_seconds()

        log("=" * 60)
        log("EXTRACTION FAILED")
        log(f"Error: {error}")
        log("=" * 60)

        return {
            "status": "FAILED",
            "source_file": str(source_file),
            "staging_file": str(staging_file),
            "source_count": 0,
            "output_count": 0,
            "difference_count": 0,
            "difference_percentage": 0.0,
            "error_message": str(error),
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration_seconds,
        }


# ============================================================
# COMMAND LINE EXECUTION
# ============================================================

def main() -> None:
    """
    Allows this module to run directly:

        python src/extract.py
    """

    result = extract_data()

    print("\n")
    print("=" * 60)
    print("EXTRACTION RESULT")
    print("=" * 60)

    for key, value in result.items():
        print(f"{key}: {value}")

    print("=" * 60)

    if result["status"] != "SUCCESS":
        sys.exit(1)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()