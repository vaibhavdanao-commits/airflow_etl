from pathlib import Path

import pandas as pd
import pyarrow as pa

from pyiceberg.catalog import load_catalog


# ============================================================
# PROJECT PATH
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


# ============================================================
# SOURCE AND TARGET
# ============================================================

CURATED_FOLDER = PROJECT_ROOT / "curated"

WAREHOUSE_FOLDER = PROJECT_ROOT / "Warehouse"

WAREHOUSE_FOLDER.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# ICEBERG CATALOG
# ============================================================

CATALOG_DB = WAREHOUSE_FOLDER / "pyiceberg_catalog.db"

WAREHOUSE_URI = (
    WAREHOUSE_FOLDER
    .resolve()
    .as_uri()
)


# ============================================================
# LOAD LOCAL ICEBERG CATALOG
# ============================================================

print()
print("=" * 100)
print("INITIALIZING APACHE ICEBERG")
print("=" * 100)

print(
    f"Warehouse : {WAREHOUSE_FOLDER}"
)

print(
    f"Catalog DB: {CATALOG_DB}"
)


catalog = load_catalog(
    "local",
    type="sql",
    uri=f"sqlite:///{CATALOG_DB}",
    warehouse=WAREHOUSE_URI
)


# ============================================================
# CREATE NAMESPACE
# ============================================================

NAMESPACE = "default"

try:

    catalog.create_namespace(
        NAMESPACE
    )

    print(
        f"Namespace created: {NAMESPACE}"
    )

except Exception:

    print(
        f"Namespace already exists: {NAMESPACE}"
    )


# ============================================================
# FIND PARQUET FILES
# ============================================================

def find_parquet_files(table_folder):

    parquet_files = list(
        table_folder.rglob("*.parquet")
    )

    return sorted(
        parquet_files
    )


# ============================================================
# CONVERT ONE CURATED DATASET
# ============================================================

def convert_to_iceberg(table_folder):

    table_name = table_folder.name

    print()
    print("=" * 100)
    print(
        f"CONVERTING TABLE: {table_name}"
    )
    print("=" * 100)

    # --------------------------------------------------------
    # Find parquet files
    # --------------------------------------------------------

    parquet_files = find_parquet_files(
        table_folder
    )

    if not parquet_files:

        print(
            f"No Parquet files found for: {table_name}"
        )

        return

    print()
    print(
        f"Parquet files found: {len(parquet_files)}"
    )

    for parquet_file in parquet_files:

        print(
            f"  - {parquet_file}"
        )

    # --------------------------------------------------------
    # Read all parquet files
    # --------------------------------------------------------

    dataframes = []

    for parquet_file in parquet_files:

        print()
        print(
            f"Reading: {parquet_file}"
        )

        df = pd.read_parquet(
            parquet_file,
            engine="pyarrow"
        )

        print(
            f"Rows read: {len(df)}"
        )

        dataframes.append(
            df
        )

    # --------------------------------------------------------
    # Combine files
    # --------------------------------------------------------

    if len(dataframes) == 1:

        combined_df = dataframes[0]

    else:

        combined_df = pd.concat(
            dataframes,
            ignore_index=True
        )

    print()
    print(
        f"Total rows: {len(combined_df)}"
    )

    print(
        f"Total columns: {len(combined_df.columns)}"
    )

    print()
    print("Columns:")

    for column in combined_df.columns:

        print(
            f"  - {column}: "
            f"{combined_df[column].dtype}"
        )

    # --------------------------------------------------------
    # Convert Pandas → PyArrow
    # --------------------------------------------------------

    print()
    print(
        "Converting Pandas DataFrame to PyArrow..."
    )

    arrow_table = pa.Table.from_pandas(
        combined_df,
        preserve_index=False
    )

    # --------------------------------------------------------
    # Iceberg table identifier
    # --------------------------------------------------------

    identifier = (
        f"{NAMESPACE}.{table_name}"
    )

    print()
    print(
        f"Iceberg table: {identifier}"
    )

    # --------------------------------------------------------
    # Check existing table
    # --------------------------------------------------------

    if catalog.table_exists(identifier):

        print()
        print(
            "Iceberg table already exists."
        )

        print(
            "Loading existing table..."
        )

        iceberg_table = catalog.load_table(
            identifier
        )

        # ----------------------------------------------------
        # Replace existing data for testing
        # ----------------------------------------------------

        print(
            "Replacing existing table data..."
        )

        iceberg_table.overwrite(
            arrow_table
        )

    else:

        # ----------------------------------------------------
        # Create new Iceberg table
        # ----------------------------------------------------

        print()
        print(
            "Creating new Iceberg table..."
        )

        iceberg_table = catalog.create_table(
            identifier=identifier,
            schema=arrow_table.schema
        )

        # ----------------------------------------------------
        # Write data
        # ----------------------------------------------------

        print(
            "Writing data to Iceberg..."
        )

        iceberg_table.append(
            arrow_table
        )

    # --------------------------------------------------------
    # Reload table
    # --------------------------------------------------------

    iceberg_table = catalog.load_table(
        identifier
    )

    # --------------------------------------------------------
    # Read Iceberg table
    # --------------------------------------------------------

    result_arrow = (
        iceberg_table
        .scan()
        .to_arrow()
    )

    result_df = result_arrow.to_pandas()

    # --------------------------------------------------------
    # Display verification
    # --------------------------------------------------------

    print()
    print("-" * 100)
    print(
        f"ICEBERG TABLE VERIFIED: {identifier}"
    )
    print("-" * 100)

    print(
        f"Rows: {len(result_df)}"
    )

    print(
        f"Columns: {len(result_df.columns)}"
    )

    print()
    print("First 10 rows:")

    print(
        result_df.head(10).to_string(
            index=False
        )
    )

    # --------------------------------------------------------
    # Snapshot information
    # --------------------------------------------------------

    print()
    print("Snapshots:")

    for snapshot in iceberg_table.snapshots():

        print(
            f"  Snapshot ID: "
            f"{snapshot.snapshot_id}"
        )

        print(
            f"  Timestamp  : "
            f"{snapshot.timestamp_ms}"
        )

        print(
            f"  Operation  : "
            f"{snapshot.summary.get('operation')}"
        )

    print()
    print(
        f"Successfully created Iceberg table: "
        f"{identifier}"
    )


# ============================================================
# DISCOVER CURATED TABLES
# ============================================================

def discover_tables():

    if not CURATED_FOLDER.exists():

        raise FileNotFoundError(
            f"Curated folder does not exist: "
            f"{CURATED_FOLDER}"
        )

    table_folders = [

        folder

        for folder in CURATED_FOLDER.iterdir()

        if folder.is_dir()

    ]

    return sorted(
        table_folders,
        key=lambda x: x.name.lower()
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 100)
    print("CURATED PARQUET → APACHE ICEBERG")
    print("=" * 100)

    print()
    print(
        f"Source : {CURATED_FOLDER}"
    )

    print(
        f"Target : {WAREHOUSE_FOLDER}"
    )

    # --------------------------------------------------------
    # Discover tables
    # --------------------------------------------------------

    table_folders = discover_tables()

    if not table_folders:

        raise Exception(
            "No curated table folders found."
        )

    print()
    print("Tables discovered:")

    for folder in table_folders:

        print(
            f"  - {folder.name}"
        )

    # --------------------------------------------------------
    # Convert each table
    # --------------------------------------------------------

    for table_folder in table_folders:

        try:

            convert_to_iceberg(
                table_folder
            )

        except Exception as e:

            print()
            print(
                f"ERROR processing "
                f"{table_folder.name}"
            )

            print(
                f"Error: {e}"
            )

            raise

    # --------------------------------------------------------
    # List Iceberg tables
    # --------------------------------------------------------

    print()
    print("=" * 100)
    print("ICEBERG TABLES CREATED")
    print("=" * 100)

    tables = catalog.list_tables(
        NAMESPACE
    )

    for table in tables:

        print(
            f"  - {table}"
        )

    # --------------------------------------------------------
    # Completed
    # --------------------------------------------------------

    print()
    print("=" * 100)
    print("CONVERSION COMPLETED")
    print("=" * 100)

    print()
    print(
        f"Iceberg Warehouse:"
    )

    print(
        WAREHOUSE_FOLDER
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()