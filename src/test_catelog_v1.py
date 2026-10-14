
import shutil
import sqlite3
from pathlib import Path

from pyiceberg.catalog import load_catalog
import pyiceberg

PROJECT_ROOT = Path(r"D:\airflow_etl")
WAREHOUSE_FOLDER = PROJECT_ROOT / "Warehouse"

ORIGINAL_DB = WAREHOUSE_FOLDER / "dimensional_model_catalog.db"
TEST_DB = WAREHOUSE_FOLDER / "dimensional_model_catalog_test_v1.db"
WAREHOUSE = WAREHOUSE_FOLDER / "dimensional_model"

print("=" * 70)
print("PYICEBERG V1 CATALOG TEST")
print("=" * 70)
print("PyIceberg version:", pyiceberg.__version__)
print("Original catalog:", ORIGINAL_DB)
print("Test catalog:", TEST_DB)

if not ORIGINAL_DB.exists():
    raise FileNotFoundError(f"Catalog not found: {ORIGINAL_DB}")

# Start with a fresh copy of the original catalog for this test.
shutil.copy2(ORIGINAL_DB, TEST_DB)

try:
    catalog = load_catalog(
        "dimensional_model",
        type="sql",
        uri=f"sqlite:///{TEST_DB.as_posix()}",
        warehouse=WAREHOUSE.resolve().as_uri(),
        schema_version="v1",
        **{
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
        },
    )

    print("\nNamespaces:", catalog.list_namespaces())

    namespace = ("star_schema",)
    print("Namespace exists:", catalog.namespace_exists(namespace))

    print("\nTables in star_schema:")
    tables = catalog.list_tables(namespace)
    for table in tables:
        print(" -", ".".join(table))

    expected = [
        "dim_customer",
        "dim_product",
        "dim_store",
        "dim_date",
        "fact_sales",
    ]

    found = {table[-1] for table in tables}
    missing = set(expected) - found

    if not missing:
        print("\nSUCCESS: All five tables are visible.")
    else:
        print("\nStill missing:", sorted(missing))

except Exception as exc:
    print("\nTEST FAILED:", type(exc).__name__, str(exc))
    print("The original catalog was not modified by this test.")

print("\nOriginal catalog remains:", ORIGINAL_DB)
print("Test catalog:", TEST_DB)
