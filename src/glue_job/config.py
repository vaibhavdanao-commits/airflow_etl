
from __future__ import annotations

from pathlib import Path
from datetime import date
from pyiceberg.catalog import load_catalog

# ============================================================
# PROJECT CONFIGURATION
# ============================================================

_THIS_FILE = Path(__file__).resolve()

PROJECT_ROOT = next(
    (
        p for p in _THIS_FILE.parents
        if (p / "Warehouse").exists()
        and (p / "curated").exists()
    ),
    _THIS_FILE.parents[2]
)

# ============================================================
# WAREHOUSE CONFIGURATION
# ============================================================

WAREHOUSE_FOLDER = PROJECT_ROOT / "Warehouse"
WAREHOUSE_FOLDER.mkdir(parents=True, exist_ok=True)

CATALOG_DB = WAREHOUSE_FOLDER / "dimensional_model_catalog.db"

DIMENSIONAL_WAREHOUSE = WAREHOUSE_FOLDER / "dimensional_model"
DIMENSIONAL_WAREHOUSE.mkdir(parents=True, exist_ok=True)

WAREHOUSE_URI = DIMENSIONAL_WAREHOUSE.resolve().as_uri()

# ============================================================
# ICEBERG CATALOG CONFIGURATION
# ============================================================

CATALOG_NAME = "dimensional_model"
NAMESPACE = "star_schema"

catalog = load_catalog(
    CATALOG_NAME,
    type="sql",
    uri=f"sqlite:///{CATALOG_DB}",
    warehouse=WAREHOUSE_URI,
    **{
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
    },
)

# ============================================================
# BUSINESS MART CONFIGURATION
# ============================================================

CURATED_FOLDER = PROJECT_ROOT / "curated"

DIMENSIONAL_WAREHOUSE_FOLDER = DIMENSIONAL_WAREHOUSE

RUN_DATE = date.today()
MAX_DATE = date(9999, 12, 31)

# ============================================================
# REGION CONFIGURATION
# ============================================================

# Add actual mappings if region is unavailable in dim_store.

REGION_BY_STORE: dict[str, str] = {
    # "STORE_001": "West",
    # "STORE_002": "South",
}

# ============================================================
# CONFIGURATION VALIDATION
# ============================================================

print("=" * 60)
print("PROJECT CONFIGURATION LOADED")
print("=" * 60)

print(f"Project Root          : {PROJECT_ROOT}")
print(f"Warehouse Folder      : {WAREHOUSE_FOLDER}")
print(f"Catalog Database      : {CATALOG_DB}")
print(f"Dimensional Warehouse : {DIMENSIONAL_WAREHOUSE}")
print(f"Curated Folder        : {CURATED_FOLDER}")
print(f"Catalog Name          : {CATALOG_NAME}")
print(f"Namespace             : {NAMESPACE}")
print(f"Run Date              : {RUN_DATE}")

print("=" * 60)

# ============================================================
# TASK 13 — SQL OPTIMIZATION CONFIGURATION
# ============================================================

TASK13_OUTPUT_FOLDER = PROJECT_ROOT / "task13_results"
TASK13_OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)




# ============================================================
# TASK 14 — PYSPARK OPTIMIZATION CONFIGURATION
# ============================================================



import os

TASK14_APP_NAME = "Task14-PySpark-Optimization"

# Output configuration
TASK14_OUTPUT_FOLDER = PROJECT_ROOT / "task14_results"

# Join optimization
TASK14_BROADCAST_PRODUCT_MAX_MB = 100

# Spark partition configuration
TASK14_SHUFFLE_PARTITIONS = max(
    200,
    int(os.cpu_count() or 4) * 2
)

TASK14_OUTPUT_PARTITIONS = 200

# Caching configuration
TASK14_CACHE_FILTERED_ORDERS = False

# Spark SQL optimization settings
TASK14_SPARK_ADAPTIVE_ENABLED = True
TASK14_SPARK_ADAPTIVE_COALESCE_ENABLED = True
TASK14_SPARK_ADAPTIVE_SKEW_JOIN_ENABLED = True

TASK14_SPARK_PARQUET_FILTER_PUSHDOWN = True
TASK14_SPARK_PARQUET_VECTORIZED_READER = True


# ============================================================
# ICEBERG TO SQLITE SYNCHRONIZATION CONFIGURATION
# ============================================================

SYNC_ICEBERG_CATALOG_DB = WAREHOUSE_FOLDER / "pyiceberg_catalog.db"
SYNC_ICEBERG_WAREHOUSE = WAREHOUSE_FOLDER

SYNC_SQLITE_DB = WAREHOUSE_FOLDER / "retail_analytics.db"

SYNC_CATALOG_NAME = "local"
SYNC_NAMESPACE = "default"

SYNC_TABLES_TO_IMPORT = [
    "customers",
    "products",
    "orders",
    "orders_enriched",
    "order_summary",
]

SYNC_STAGING_PREFIX = "_staging_"

# Separate catalog for the SQLite synchronization source
sync_iceberg_catalog = load_catalog(
    SYNC_CATALOG_NAME,
    type="sql",
    uri=f"sqlite:///{SYNC_ICEBERG_CATALOG_DB}",
    warehouse=SYNC_ICEBERG_WAREHOUSE.resolve().as_uri(),
    **{
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
    },
)
# ============================================================
# CURATED PARQUET TO ICEBERG — INCREMENTAL CONFIGURATION
# ============================================================

INCREMENTAL_PROCESSED_FILES_METADATA = (
    WAREHOUSE_FOLDER / "iceberg_processed_files.json"
)

INCREMENTAL_CATALOG_DB = (
    WAREHOUSE_FOLDER / "pyiceberg_catalog.db"
)

INCREMENTAL_WAREHOUSE_URI = (
    WAREHOUSE_FOLDER.resolve().as_uri()
)

INCREMENTAL_CATALOG_NAME = "local"
INCREMENTAL_NAMESPACE = "default"

# Separate catalog for incremental curated-to-Iceberg ingestion
incremental_catalog = load_catalog(
    INCREMENTAL_CATALOG_NAME,
    type="sql",
    uri=f"sqlite:///{INCREMENTAL_CATALOG_DB}",
    warehouse=INCREMENTAL_WAREHOUSE_URI,
    **{
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
    },
)