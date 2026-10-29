
import sqlite3
from pathlib import Path

db_path = Path(
    r"D:\airflow_etl\Warehouse\dimensional_model_catalog.db"
)

print("Catalog exists:", db_path.is_file())

if not db_path.is_file():
    raise FileNotFoundError(db_path)

with sqlite3.connect(str(db_path)) as conn:
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()

    print("\nCatalog tables:")
    for table in tables:
        print(" -", table[0])

    print("\nSchema for iceberg_tables:")
    columns = conn.execute(
        "PRAGMA table_info(iceberg_tables)"
    ).fetchall()

    for column in columns:
        print(column)

    print("\nRecord for dim_customer:")
    column_names = [column[1] for column in columns]

    if "table_name" in column_names:
        rows = conn.execute(
            "SELECT * FROM iceberg_tables WHERE table_name = ?",
            ("dim_customer",),
        ).fetchall()

        for row in rows:
            for name, value in zip(column_names, row):
                print(f"{name}: {value}")
    else:
        print("No table_name column found.")
