from pathlib import Path
import sqlite3

from pyiceberg.catalog import load_catalog


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(r"D:\airflow_etl").resolve()

WAREHOUSE = (
    PROJECT_ROOT
    / "Warehouse"
    / "dimensional_model"
).resolve()

CATALOG_DB = (
    PROJECT_ROOT
    / "Warehouse"
    / "dimensional_model_catalog.db"
).resolve()

CATALOG_NAME = "local_dimensional_model"

NAMESPACE = "star_schema"


# ============================================================
# PRINT CONFIGURATION
# ============================================================

print("=" * 70)
print("PYICEBERG CATALOG TEST")
print("=" * 70)

print(f"Project Root : {PROJECT_ROOT}")
print(f"Catalog DB   : {CATALOG_DB}")
print(f"Warehouse    : {WAREHOUSE}")
print(f"Namespace    : {NAMESPACE}")

print()
print("Catalog exists :", CATALOG_DB.exists())
print("Warehouse exists:", WAREHOUSE.exists())

if not CATALOG_DB.exists():
    raise FileNotFoundError(
        f"\nCatalog database not found:\n{CATALOG_DB}"
    )


# ============================================================
# CHECK SQLITE DATABASE DIRECTLY
# ============================================================

print()
print("=" * 70)
print("1. DIRECT SQLITE CHECK")
print("=" * 70)

conn = sqlite3.connect(str(CATALOG_DB))

cursor = conn.cursor()

tables = cursor.execute(
    """
    SELECT name
    FROM sqlite_master
    WHERE type='table'
    ORDER BY name
    """
).fetchall()

print("\nSQLite tables:")
for table in tables:
    print("  ", table[0])


# ============================================================
# CHECK ALL SQLITE TABLE CONTENTS
# ============================================================

print()
print("=" * 70)
print("2. SQLITE TABLE ROW COUNTS")
print("=" * 70)

for (table_name,) in tables:

    try:
        count = cursor.execute(
            f'SELECT COUNT(*) FROM "{table_name}"'
        ).fetchone()[0]

        print(f"{table_name}: {count}")

    except Exception as exc:
        print(
            f"{table_name}: ERROR -> {exc}"
        )


# ============================================================
# PRINT NAMESPACE TABLE
# ============================================================

print()
print("=" * 70)
print("3. NAMESPACE INFORMATION")
print("=" * 70)

try:

    namespace_rows = cursor.execute(
        """
        SELECT *
        FROM namespaces
        """
    ).fetchall()

    print("Namespaces:")
    for row in namespace_rows:
        print("  ", row)

except Exception as exc:

    print(
        "Could not read namespaces table:"
    )
    print(exc)


# ============================================================
# PRINT ICEBERG TABLE REGISTRY
# ============================================================

print()
print("=" * 70)
print("4. ICEBERG TABLE REGISTRY")
print("=" * 70)

try:

    iceberg_rows = cursor.execute(
        """
        SELECT *
        FROM iceberg_tables
        """
    ).fetchall()

    print("Iceberg tables:")

    for row in iceberg_rows:
        print("  ", row)

except Exception as exc:

    print(
        "Could not read iceberg_tables:"
    )
    print(exc)


conn.close()


# ============================================================
# LOAD PYICEBERG CATALOG
# ============================================================

print()
print("=" * 70)
print("5. PYICEBERG CATALOG")
print("=" * 70)

catalog = load_catalog(
    CATALOG_NAME,
    **{
        "type": "sql",
        "uri": f"sqlite:///{CATALOG_DB.as_posix()}",
        "warehouse": WAREHOUSE.as_uri(),
    },
)

print("Catalog loaded successfully.")


# ============================================================
# LIST NAMESPACES
# ============================================================

print()
print("=" * 70)
print("6. PYICEBERG NAMESPACES")
print("=" * 70)

try:

    namespaces = catalog.list_namespaces()

    print("Namespaces returned by PyIceberg:")

    for namespace in namespaces:
        print("  ", namespace)

except Exception as exc:

    print(
        "ERROR listing namespaces:"
    )
    print(exc)


# ============================================================
# CHECK STAR_SCHEMA
# ============================================================

print()
print("=" * 70)
print("7. STAR_SCHEMA CHECK")
print("=" * 70)

try:

    tables = catalog.list_tables(NAMESPACE)

    print(
        f"Tables in namespace '{NAMESPACE}':"
    )

    for table in tables:
        print("  ", table)

except Exception as exc:

    print(
        f"ERROR listing namespace '{NAMESPACE}':"
    )
    print(exc)


# ============================================================
# CHECK REQUIRED TABLES
# ============================================================

print()
print("=" * 70)
print("8. REQUIRED TABLE CHECK")
print("=" * 70)

required_tables = [
    "dim_customer",
    "dim_product",
    "dim_store",
    "dim_date",
    "fact_sales",
]

try:

    available_tables = {
        table[-1]
        for table in catalog.list_tables(NAMESPACE)
    }

except Exception:

    available_tables = set()


for table_name in required_tables:

    full_name = f"{NAMESPACE}.{table_name}"

    if table_name in available_tables:
        print(f"[OK]      {full_name}")
    else:
        print(f"[MISSING] {full_name}")


# ============================================================
# FINISHED
# ============================================================

print()
print("=" * 70)
print("TEST COMPLETED")
print("=" * 70)