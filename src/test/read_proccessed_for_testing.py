from pathlib import Path
import pandas as pd

# ============================================================
# PROJECT PATH
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# ============================================================
# OUTPUT FOLDERS
# ============================================================

PROCESSED_FOLDER = PROJECT_ROOT / "processed"
CURATED_FOLDER = PROJECT_ROOT / "curated"
QUARANTINE_FOLDER = PROJECT_ROOT / "quarantine"

# ============================================================
# DISPLAY SETTINGS
# ============================================================

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 50)


# ============================================================
# FIND ALL PARQUET FILES
# ============================================================

def find_parquet_files(folder):

    if not folder.exists():
        print(f"Folder does not exist: {folder}")
        return []

    parquet_files = list(folder.rglob("*.parquet"))

    return parquet_files


# ============================================================
# DISPLAY PARQUET FILE
# ============================================================

def display_parquet_file(parquet_file):

    print()
    print("=" * 100)
    print("PARQUET FILE")
    print("=" * 100)

    print(f"File       : {parquet_file}")
    print(f"File Name  : {parquet_file.name}")
    print(f"File Size  : {parquet_file.stat().st_size / 1024:.2f} KB")

    print()
    print("-" * 100)
    print("READING FILE")
    print("-" * 100)

    try:

        df = pd.read_parquet(
            parquet_file,
            engine="pyarrow"
        )

    except Exception as e:

        print()
        print(f"ERROR reading file: {parquet_file}")
        print(f"Error details: {e}")

        return

    # ========================================================
    # BASIC INFORMATION
    # ========================================================

    print()
    print("-" * 100)
    print("DATASET INFORMATION")
    print("-" * 100)

    print(f"Rows       : {len(df)}")
    print(f"Columns    : {len(df.columns)}")

    # ========================================================
    # COLUMNS
    # ========================================================

    print()
    print("-" * 100)
    print("COLUMNS")
    print("-" * 100)

    for index, column in enumerate(df.columns, start=1):

        print(
            f"{index:3}. {column}"
        )

    # ========================================================
    # DATA TYPES
    # ========================================================

    print()
    print("-" * 100)
    print("DATA TYPES")
    print("-" * 100)

    print(
        df.dtypes.to_string()
    )

    # ========================================================
    # NULL COUNTS
    # ========================================================

    print()
    print("-" * 100)
    print("NULL VALUES")
    print("-" * 100)

    null_counts = df.isnull().sum()

    null_counts = null_counts[
        null_counts > 0
    ]

    if null_counts.empty:

        print("No null values found.")

    else:

        print(
            null_counts.to_string()
        )

    # ========================================================
    # FIRST 5 RECORDS
    # ========================================================

    print()
    print("-" * 100)
    print("FIRST 5 RECORDS")
    print("-" * 100)

    if df.empty:

        print("Dataset is empty.")

    else:

        print(
            df.head(5).to_string(index=False)
        )

    # ========================================================
    # LAST 5 RECORDS
    # ========================================================

    print()
    print("-" * 100)
    print("LAST 5 RECORDS")
    print("-" * 100)

    if df.empty:

        print("Dataset is empty.")

    else:

        print(
            df.tail(5).to_string(index=False)
        )

    # ========================================================
    # SUMMARY
    # ========================================================

    print()
    print("-" * 100)
    print("SUMMARY")
    print("-" * 100)

    print(
        df.info()
    )

    print()
    print("=" * 100)
    print("FILE READ SUCCESSFULLY")
    print("=" * 100)


# ============================================================
# PROCESS FOLDER
# ============================================================

def process_folder(folder, folder_name):

    print()
    print()
    print("#" * 100)
    print(f"# {folder_name.upper()} PARQUET FILES")
    print("#" * 100)

    parquet_files = find_parquet_files(
        folder
    )

    if not parquet_files:

        print()
        print(
            f"No Parquet files found under: {folder}"
        )

        return

    print()
    print(
        f"Parquet files found: {len(parquet_files)}"
    )

    # ========================================================
    # DISPLAY FILE LIST
    # ========================================================

    print()

    for index, file in enumerate(
        parquet_files,
        start=1
    ):

        print(
            f"{index}. {file}"
        )

    # ========================================================
    # READ EVERY PARQUET FILE
    # ========================================================

    for parquet_file in parquet_files:

        display_parquet_file(
            parquet_file
        )


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 100)
    print("PARQUET FILE READER - TESTING")
    print("=" * 100)

    print()
    print(f"Project Root : {PROJECT_ROOT}")

    print()
    print("Folders:")
    print(
        f"Processed    : {PROCESSED_FOLDER}"
    )
    print(
        f"Curated      : {CURATED_FOLDER}"
    )
    print(
        f"Quarantine   : {QUARANTINE_FOLDER}"
    )

    # ========================================================
    # PROCESSED
    # ========================================================

    process_folder(
        PROCESSED_FOLDER,
        "Processed"
    )

    # ========================================================
    # CURATED
    # ========================================================

    process_folder(
        CURATED_FOLDER,
        "Curated"
    )

    # ========================================================
    # QUARANTINE
    # ========================================================

    process_folder(
        QUARANTINE_FOLDER,
        "Quarantine"
    )

    # ========================================================
    # COMPLETED
    # ========================================================

    print()
    print()
    print("=" * 100)
    print("ALL PARQUET FILES READ SUCCESSFULLY")
    print("=" * 100)
    print()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()