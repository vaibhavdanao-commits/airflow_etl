
# ============================================================
# sync_iceberg_to_sqlite.py
# Full refresh: Apache Iceberg -> SQLite
# Project: D:\airflow_etl
# ============================================================

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from pyiceberg.catalog import load_catalog


# ============================================================
# 1. WINDOWS CONSOLE ENCODING
# ============================================================

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass


# ============================================================
# 2. PROJECT PATHS
# Script location:
# D:\airflow_etl\src\glue_job\sync_iceberg_to_sqlite.py
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

WAREHOUSE_FOLDER = PROJECT_ROOT / "Warehouse"

ICEBERG_CATALOG_DB = WAREHOUSE_FOLDER / "pyiceberg_catalog.db"
ICEBERG_WAREHOUSE = WAREHOUSE_FOLDER

SQLITE_DB = WAREHOUSE_FOLDER / "retail_analytics.db"


# ============================================================
# 3. ICEBERG CONFIGURATION
# Matches your reference script
# ============================================================

CATALOG_NAME = "local"
NAMESPACE = "default"

TABLES_TO_IMPORT = [
    "customers",
    "products",
    "orders",
    "orders_enriched",
    "order_summary",
]


# ============================================================
# 4. GENERAL CONFIGURATION
# ============================================================

STAGING_PREFIX = "_staging_"


# ============================================================
# 5. PRINT CONFIGURATION
# ============================================================

def print_configuration() -> None:
    print("\n" + "=" * 70)
    print("ICEBERG TO SQLITE DATA SYNCHRONIZATION")
    print("=" * 70)

    print(f"Project root       : {PROJECT_ROOT}")
    print(f"Iceberg catalog DB : {ICEBERG_CATALOG_DB}")
    print(f"Iceberg warehouse  : {ICEBERG_WAREHOUSE}")
    print(f"SQLite database    : {SQLITE_DB}")
    print(f"Catalog name       : {CATALOG_NAME}")
    print(f"Namespace          : {NAMESPACE}")

    print("\nTables to import:")
    for table_name in TABLES_TO_IMPORT:
        print(f"  - {table_name}")

    print("=" * 70)


# ============================================================
# 6. VALIDATE PATHS
# ============================================================

def validate_paths() -> None:
    if not PROJECT_ROOT.exists():
        raise FileNotFoundError(
            f"Project directory does not exist:\n{PROJECT_ROOT}"
        )

    if not ICEBERG_CATALOG_DB.exists():
        raise FileNotFoundError(
            "Iceberg catalog database not found:\n"
            f"{ICEBERG_CATALOG_DB}\n\n"
            "Check the catalog database path used in your reference "
            "script. This script expects:\n"
            r"D:\airflow_etl\Warehouse\pyiceberg_catalog.db"
        )

    WAREHOUSE_FOLDER.mkdir(parents=True, exist_ok=True)

    print("\nPath validation successful.")


# ============================================================
# 7. CONNECT TO EXISTING ICEBERG CATALOG
# ============================================================

def connect_iceberg_catalog():
    print("\nConnecting to the existing Iceberg catalog...")

    catalog = load_catalog(
        CATALOG_NAME,
        type="sql",
        uri=f"sqlite:///{ICEBERG_CATALOG_DB}",
        warehouse=ICEBERG_WAREHOUSE.resolve().as_uri(),
        **{
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
        },
    )

    print("Iceberg catalog connection successful.")
    return catalog


# ============================================================
# 8. CONVERT VALUES TO SQLITE-FRIENDLY VALUES
# ============================================================

def normalize_value(value: Any) -> Any:
    """
    Convert Pandas, NumPy, Arrow-derived and Python values into
    values that SQLite can store reliably.
    """

    if value is None or value is pd.NA or value is pd.NaT:
        return None

    # NumPy scalar -> Python scalar
    if isinstance(value, np.generic):
        value = value.item()

    # Handle missing scalar values without treating lists or
    # dictionaries as scalar values.
    if not isinstance(value, (list, dict, tuple, set)):
        try:
            missing = pd.isna(value)
            if isinstance(missing, (bool, np.bool_)) and missing:
                return None
        except (TypeError, ValueError):
            pass

    # Date/time values -> ISO strings
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()

    # Decimal values -> text, preserving decimal precision
    if isinstance(value, Decimal):
        return str(value)

    # Boolean values -> SQLite integer
    if isinstance(value, (bool, np.bool_)):
        return int(value)

    # SQLite does not natively store nested structures
    if isinstance(value, (dict, list, tuple, set)):
        if isinstance(value, set):
            value = list(value)

        return json.dumps(
            value,
            default=str,
            ensure_ascii=False,
        )

    # Keep binary data as binary
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)

    # SQLite supports strings, integers, floats and None
    if isinstance(value, (str, int, float)):
        return value

    # Fallback for uncommon data types
    return str(value)


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize all columns before writing to SQLite.
    """

    df = df.copy()

    # SQLite column names are preserved as supplied by Iceberg.
    # Nested objects are serialized as JSON strings.
    for column in df.columns:
        df[column] = df[column].map(normalize_value)

    return df


# ============================================================
# 9. LOAD ONE ICEBERG TABLE INTO PANDAS
# ============================================================

def read_iceberg_table(catalog, table_name: str) -> pd.DataFrame:
    identifier = (NAMESPACE, table_name)

    print(f"\nReading Iceberg table: {NAMESPACE}.{table_name}")

    iceberg_table = catalog.load_table(identifier)

    # Read the complete table. This is a full refresh, not an
    # incremental scan based on processed-file metadata.
    arrow_table = iceberg_table.scan().to_arrow()

    df = arrow_table.to_pandas()

    print(f"  Rows read    : {len(df):,}")
    print(f"  Columns      : {len(df.columns)}")

    return normalize_dataframe(df)


# ============================================================
# 10. OPEN SQLITE CONNECTION
# ============================================================

def connect_sqlite() -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(SQLITE_DB),
        timeout=60,
    )

    conn.execute("PRAGMA busy_timeout = 60000")
    conn.execute("PRAGMA foreign_keys = OFF")

    # WAL helps concurrent readers while the database is updated.
    conn.execute("PRAGMA journal_mode = WAL")

    return conn


# ============================================================
# 11. CREATE STAGING TABLES
# Existing production tables remain untouched at this stage.
# ============================================================

def write_staging_table(
    df: pd.DataFrame,
    table_name: str,
    conn: sqlite3.Connection,
) -> int:
    staging_table = f"{STAGING_PREFIX}{table_name}"

    print(f"Writing SQLite staging table: {staging_table}")

    # Remove leftover staging table from a previous failed run.
    conn.execute(
        f'DROP TABLE IF EXISTS "{staging_table}"'
    )

    df.to_sql(
        name=staging_table,
        con=conn,
        if_exists="replace",
        index=False,
        chunksize=10_000,
    )

    result = conn.execute(
        f'SELECT COUNT(*) FROM "{staging_table}"'
    ).fetchone()

    sqlite_count = int(result[0])

    expected_count = len(df)

    print(f"  Expected rows: {expected_count:,}")
    print(f"  SQLite rows  : {sqlite_count:,}")

    if sqlite_count != expected_count:
        raise RuntimeError(
            f"Row count mismatch for {table_name}: "
            f"Iceberg/Pandas={expected_count}, "
            f"SQLite staging={sqlite_count}"
        )

    return sqlite_count


# ============================================================
# 12. ATOMICALLY REPLACE TARGET TABLES
# All target-table swaps happen inside one SQLite transaction.
# ============================================================

def replace_target_tables(
    conn: sqlite3.Connection,
    row_counts: dict[str, int],
) -> None:
    print("\nRefreshing SQLite target tables...")

    try:
        conn.execute("BEGIN IMMEDIATE")

        for table_name in TABLES_TO_IMPORT:
            staging_table = f"{STAGING_PREFIX}{table_name}"

            # Remove old target, if present.
            conn.execute(
                f'DROP TABLE IF EXISTS "{table_name}"'
            )

            # Promote the staging table to the final table name.
            conn.execute(
                f'ALTER TABLE "{staging_table}" '
                f'RENAME TO "{table_name}"'
            )

            print(
                f"  Refreshed {table_name}: "
                f"{row_counts[table_name]:,} rows"
            )

        conn.commit()

    except Exception:
        conn.rollback()
        raise


# ============================================================
# 13. CLEAN UP LEFTOVER STAGING TABLES
# ============================================================

def cleanup_staging_tables(
    conn: sqlite3.Connection,
) -> None:
    for table_name in TABLES_TO_IMPORT:
        staging_table = f"{STAGING_PREFIX}{table_name}"

        conn.execute(
            f'DROP TABLE IF EXISTS "{staging_table}"'
        )

    conn.commit()


# ============================================================
# 14. VERIFY FINAL SQLITE TABLES
# ============================================================

def verify_sqlite_tables(
    conn: sqlite3.Connection,
    expected_counts: dict[str, int],
) -> None:
    print("\n" + "=" * 70)
    print("FINAL SQLITE VERIFICATION")
    print("=" * 70)

    failures = []

    for table_name in TABLES_TO_IMPORT:
        result = conn.execute(
            f'SELECT COUNT(*) FROM "{table_name}"'
        ).fetchone()

        actual_count = int(result[0])
        expected_count = expected_counts[table_name]

        status = (
            "PASS"
            if actual_count == expected_count
            else "FAIL"
        )

        print(
            f"{table_name:<20} "
            f"Rows: {actual_count:>12,} "
            f"Expected: {expected_count:>12,} "
            f"[{status}]"
        )

        if actual_count != expected_count:
            failures.append(table_name)

    if failures:
        raise RuntimeError(
            "Final SQLite verification failed for: "
            + ", ".join(failures)
        )

    print("=" * 70)
    print("All SQLite tables verified successfully.")


# ============================================================
# 15. MAIN EXECUTION
# ============================================================

def main() -> None:
    print_configuration()
    validate_paths()

    catalog = connect_iceberg_catalog()

    # Validate that the configured namespace exists.
    namespaces = catalog.list_namespaces()

    if (NAMESPACE,) not in namespaces:
        raise RuntimeError(
            f"Namespace '{NAMESPACE}' was not found.\n"
            f"Namespaces reported by the catalog: {namespaces}\n"
            "Verify CATALOG_NAME and NAMESPACE against your "
            "reference script."
        )

    # Confirm that every requested table exists before syncing.
    available_tables = {
        identifier[-1]
        for identifier in catalog.list_tables(NAMESPACE)
    }

    missing_tables = [
        name
        for name in TABLES_TO_IMPORT
        if name not in available_tables
    ]

    if missing_tables:
        raise RuntimeError(
            "These tables were not found in the Iceberg namespace "
            f"'{NAMESPACE}': {missing_tables}\n"
            f"Available tables: {sorted(available_tables)}"
        )

    conn = connect_sqlite()

    expected_counts: dict[str, int] = {}
    staging_complete = False

    try:
        # Phase 1: Read all Iceberg tables and create staging tables.
        # Existing target tables are not replaced until every table
        # has been staged and verified.
        for table_name in TABLES_TO_IMPORT:
            df = read_iceberg_table(catalog, table_name)

            expected_counts[table_name] = write_staging_table(
                df=df,
                table_name=table_name,
                conn=conn,
            )

            del df

        staging_complete = True

        # Phase 2: Replace all target tables in one transaction.
        replace_target_tables(
            conn=conn,
            row_counts=expected_counts,
        )

        # Phase 3: Validate the final SQLite tables.
        verify_sqlite_tables(
            conn=conn,
            expected_counts=expected_counts,
        )

        print("\n" + "=" * 70)
        print("SYNCHRONIZATION COMPLETED SUCCESSFULLY")
        print("=" * 70)
        print(f"SQLite database: {SQLITE_DB}")
        print(f"Tables synced  : {len(TABLES_TO_IMPORT)}")

        for table_name in TABLES_TO_IMPORT:
            print(
                f"  {table_name:<20} "
                f"{expected_counts[table_name]:>12,} rows"
            )

        print("\nYour Streamlit application can now connect to:")
        print(SQLITE_DB)
        print("=" * 70)

    finally:
        try:
            # If staging failed, clean up incomplete staging tables.
            # If table replacement failed, SQLite rolls back the
            # target-table swaps; cleanup removes leftover staging.
            cleanup_staging_tables(conn)
        except sqlite3.Error as exc:
            print(
                f"\nWarning: staging cleanup could not complete: {exc}"
            )

        conn.close()


if __name__ == "__main__":
    main()
