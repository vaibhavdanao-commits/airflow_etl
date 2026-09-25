from pathlib import Path

# ============================================================
# AWS CONFIGURATION
# ============================================================

S3_BUCKET = "airflow-etl-source"
S3_PREFIX = "raw"


# ============================================================
# LOCAL DATA CONFIGURATION
# ============================================================

LOCAL_FOLDER = Path(r"D:\airflow_etl\raw")


# ============================================================
# LOCAL ETL OUTPUT CONFIGURATION
# ============================================================

BASE_FOLDER = LOCAL_FOLDER.parent

PROCESSED_FOLDER = BASE_FOLDER / "processed"
CURATED_FOLDER = BASE_FOLDER / "curated"
QUARANTINE_FOLDER = BASE_FOLDER / "quarantine"


# ============================================================
# CREATE OUTPUT DIRECTORIES
# ============================================================

PROCESSED_FOLDER.mkdir(parents=True, exist_ok=True)
CURATED_FOLDER.mkdir(parents=True, exist_ok=True)
QUARANTINE_FOLDER.mkdir(parents=True, exist_ok=True)


# ============================================================
# DISPLAY CONFIGURATION
# ============================================================

print("=" * 80)
print("LOCAL ETL CONFIGURATION")
print("=" * 80)

print(f"S3 Bucket         : {S3_BUCKET}")
print(f"S3 Prefix         : {S3_PREFIX}")
print(f"Raw Folder        : {LOCAL_FOLDER}")
print(f"Processed Folder  : {PROCESSED_FOLDER}")
print(f"Curated Folder    : {CURATED_FOLDER}")
print(f"Quarantine Folder : {QUARANTINE_FOLDER}")

print("=" * 80)
