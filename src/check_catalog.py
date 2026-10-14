
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(r"D:\airflow_etl")

CATALOG_DB = (
    PROJECT_ROOT
    / "Warehouse"
    / "dimensional_model_catalog.db"
)

print("=" * 80)
print("ICEBERG NAMESPACE REGISTRATION DIAGNOSTIC")
print("=" * 80)
print("Catalog:", CATALOG_DB)

if not CATALOG_DB.exists():
    raise FileNotFoundError(CATALOG_DB)

with sqlite3.connect(
    f"file:{CATALOG_DB.as_posix()}?mode=ro",
    uri=True,
) as conn:

    conn.row_factory = sqlite3.Row

    print("\n1. Namespace properties schema")
    columns = conn.execute(
        "PRAGMA table_info(iceberg_namespace_properties)"
    ).fetchall()

    for column in columns:
        print(dict(column))

    print("\n2. Namespace properties rows")
    rows = conn.execute(
        "SELECT * FROM iceberg_namespace_properties"
    ).fetchall()

    for row in rows:
        print(dict(row))

    print("\n3. Table registry")
    rows = conn.execute(
        "SELECT * FROM iceberg_tables"
    ).fetchall()

    for row in rows:
        print(dict(row))

    print("\n4. Catalog names in table registry")
    rows = conn.execute(
        """
        SELECT DISTINCT catalog_name
        FROM iceberg_tables
        """
    ).fetchall()

    for row in rows:
        print(row["catalog_name"])

print("\nDiagnostic completed. No database changes made.")
