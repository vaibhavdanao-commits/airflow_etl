from pathlib import Path

import polars as pl

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    NestedField,
)


# ============================================================
# PROJECT PATH
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parent
    .parent
    .parent
)


# ============================================================
# SOURCE AND TARGET
# ============================================================

CURATED_FOLDER = (
    PROJECT_ROOT / "curated"
)

WAREHOUSE_FOLDER = (
    PROJECT_ROOT / "Warehouse"
)

WAREHOUSE_FOLDER.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# ICEBERG CATALOG
# ============================================================

CATALOG_DB = (
    WAREHOUSE_FOLDER /
    "pyiceberg_catalog.db"
)

WAREHOUSE_URI = (
    WAREHOUSE_FOLDER
    .resolve()
    .as_uri()
)

NAMESPACE = "default"


# ============================================================
# INITIALIZE ICEBERG CATALOG
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
    warehouse=WAREHOUSE_URI,
    **{
        "py-io-impl":
        "pyiceberg.io.fsspec.FsspecFileIO"
    }
)


# ============================================================
# CREATE NAMESPACE
# ============================================================

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

def find_parquet_files(
    table_folder
):

    return sorted(
        table_folder.rglob(
            "*.parquet"
        )
    )


# ============================================================
# READ PARQUET FILES USING POLARS
# ============================================================

def read_parquet_files(
    parquet_files
):

    dataframes = []

    for parquet_file in parquet_files:

        print()
        print(
            f"Reading: {parquet_file}"
        )

        df = pl.read_parquet(
            parquet_file
        )

        print(
            f"Rows read: {df.height}"
        )

        dataframes.append(
            df
        )

    if not dataframes:

        raise Exception(
            "No Parquet files found."
        )

    if len(dataframes) == 1:

        return dataframes[0]

    return pl.concat(
        dataframes,
        how="vertical_relaxed"
    )


# ============================================================
# PREPARE DATAFRAME
# ============================================================

def prepare_dataframe(
    df
):

    # --------------------------------------------------------
    # Remove unwanted index columns
    # --------------------------------------------------------

    remove_columns = []

    for column in df.columns:

        if (
            column.startswith(
                "__index"
            )
            or column.startswith(
                "Unnamed:"
            )
        ):

            remove_columns.append(
                column
            )

    if remove_columns:

        print()
        print(
            "Removing unwanted columns:"
        )

        for column in remove_columns:

            print(
                f"  - {column}"
            )

        df = df.drop(
            remove_columns
        )

    # --------------------------------------------------------
    # Convert Object columns
    # --------------------------------------------------------

    for column in df.columns:

        if df[column].dtype == pl.Object:

            df = df.with_columns(
                pl.col(column)
                .cast(pl.String)
            )

    return df


# ============================================================
# POLARS TYPE → ICEBERG TYPE
# ============================================================

def polars_dtype_to_iceberg(
    dtype
):

    # --------------------------------------------------------
    # Boolean
    # --------------------------------------------------------

    if dtype == pl.Boolean:

        return BooleanType()


    # --------------------------------------------------------
    # Signed integers
    # --------------------------------------------------------

    if dtype == pl.Int8:

        return IntegerType()

    if dtype == pl.Int16:

        return IntegerType()

    if dtype == pl.Int32:

        return IntegerType()

    if dtype == pl.Int64:

        return LongType()


    # --------------------------------------------------------
    # Unsigned integers
    # --------------------------------------------------------

    if dtype == pl.UInt8:

        return IntegerType()

    if dtype == pl.UInt16:

        return IntegerType()

    if dtype == pl.UInt32:

        return LongType()

    if dtype == pl.UInt64:

        return LongType()


    # --------------------------------------------------------
    # Floating point
    # --------------------------------------------------------

    if dtype == pl.Float32:

        return FloatType()

    if dtype == pl.Float64:

        return DoubleType()


    # --------------------------------------------------------
    # String
    # --------------------------------------------------------

    if dtype == pl.String:

        return StringType()


    # --------------------------------------------------------
    # Date
    # --------------------------------------------------------

    if dtype == pl.Date:

        return DateType()


    # --------------------------------------------------------
    # Datetime
    # --------------------------------------------------------

    if isinstance(
        dtype,
        pl.Datetime
    ):

        return TimestampType()


    # --------------------------------------------------------
    # Categorical
    # --------------------------------------------------------

    if dtype == pl.Categorical:

        return StringType()


    # --------------------------------------------------------
    # Enum
    # --------------------------------------------------------

    if dtype == pl.Enum:

        return StringType()


    # --------------------------------------------------------
    # Unsupported types
    # --------------------------------------------------------

    return StringType()


# ============================================================
# CREATE ICEBERG SCHEMA
# ============================================================

def create_iceberg_schema(
    df
):

    fields = []

    field_id = 1

    print()
    print(
        "Creating Iceberg schema:"
    )

    for column in df.columns:

        polars_dtype = (
            df[column].dtype
        )

        iceberg_type = (
            polars_dtype_to_iceberg(
                polars_dtype
            )
        )

        field = NestedField(
            field_id=field_id,
            name=column,
            field_type=iceberg_type,
            required=False
        )

        fields.append(
            field
        )

        print(
            f"  {field_id}. "
            f"{column} : "
            f"{polars_dtype} → "
            f"{iceberg_type}"
        )

        field_id += 1

    return Schema(
        *fields
    )


# ============================================================
# CREATE ICEBERG TABLE
# ============================================================

def create_iceberg_table(
    table_name,
    df
):

    identifier = (
        f"{NAMESPACE}.{table_name}"
    )

    print()
    print(
        f"Iceberg table: {identifier}"
    )

    # --------------------------------------------------------
    # Existing table
    # --------------------------------------------------------

    if catalog.table_exists(
        identifier
    ):

        print()
        print(
            "Iceberg table already exists."
        )

        return catalog.load_table(
            identifier
        )


    # --------------------------------------------------------
    # Create schema
    # --------------------------------------------------------

    iceberg_schema = (
        create_iceberg_schema(
            df
        )
    )


    # --------------------------------------------------------
    # Create table
    # --------------------------------------------------------

    print()
    print(
        "Creating Iceberg table..."
    )

    iceberg_table = (
        catalog.create_table(
            identifier=identifier,
            schema=iceberg_schema
        )
    )

    print()
    print(
        "Iceberg table created:"
    )

    print(
        iceberg_table
    )

    return iceberg_table


# ============================================================
# CONVERT DATAFRAME TO PYARROW
# ============================================================

def dataframe_to_arrow(
    df
):

    # Import only here so the rest of the script can
    # initialize the Fsspec catalog first.

    import pyarrow as pa

    return pa.Table.from_pandas(
        df.to_pandas(),
        preserve_index=False
    )


# ============================================================
# WRITE DATA TO ICEBERG
# ============================================================

def write_to_iceberg(
    df,
    iceberg_table,
    overwrite=False
):

    print()

    if overwrite:

        print(
            "Overwriting Iceberg table..."
        )

    else:

        print(
            "Appending data to Iceberg table..."
        )


    # --------------------------------------------------------
    # Convert Polars → PyArrow
    # --------------------------------------------------------

    arrow_table = (
        dataframe_to_arrow(
            df
        )
    )


    print(
        f"Arrow rows: "
        f"{arrow_table.num_rows}"
    )


    # --------------------------------------------------------
    # Write through PyIceberg
    # --------------------------------------------------------

    if overwrite:

        iceberg_table.overwrite(
            arrow_table
        )

    else:

        iceberg_table.append(
            arrow_table
        )


    print()
    print(
        "Data successfully written to Iceberg."
    )


# ============================================================
# VERIFY ICEBERG TABLE
# ============================================================

def verify_iceberg_table(
    identifier
):

    print()
    print(
        "Reading Iceberg table for verification..."
    )


    iceberg_table = (
        catalog.load_table(
            identifier
        )
    )


    # --------------------------------------------------------
    # Scan table
    # --------------------------------------------------------

    result_arrow = (
        iceberg_table
        .scan()
        .to_arrow()
    )


    result_df = (
        result_arrow
        .to_pandas()
    )


    # --------------------------------------------------------
    # Verification
    # --------------------------------------------------------

    print()
    print("-" * 100)

    print(
        f"ICEBERG TABLE VERIFIED: "
        f"{identifier}"
    )

    print("-" * 100)

    print(
        f"Rows    : "
        f"{len(result_df)}"
    )

    print(
        f"Columns : "
        f"{len(result_df.columns)}"
    )


    # --------------------------------------------------------
    # Schema
    # --------------------------------------------------------

    print()
    print(
        "Iceberg Schema:"
    )

    print(
        iceberg_table.schema()
    )


    # --------------------------------------------------------
    # First 10 rows
    # --------------------------------------------------------

    print()
    print(
        "First 10 rows:"
    )

    print(
        result_df
        .head(10)
        .to_string(
            index=False
        )
    )


    # --------------------------------------------------------
    # Snapshots
    # --------------------------------------------------------

    print()
    print(
        "Snapshots:"
    )

    try:

        snapshots = list(
            iceberg_table.snapshots()
        )

        if not snapshots:

            print(
                "  No snapshots found."
            )

        else:

            for snapshot in snapshots:

                print()

                print(
                    f"  Snapshot ID: "
                    f"{snapshot.snapshot_id}"
                )

                print(
                    f"  Timestamp  : "
                    f"{snapshot.timestamp_ms}"
                )

                try:

                    operation = (
                        snapshot
                        .summary
                        .get(
                            "operation"
                        )
                    )

                except Exception:

                    operation = "N/A"

                print(
                    f"  Operation  : "
                    f"{operation}"
                )

    except Exception as e:

        print(
            f"  Unable to read snapshots: "
            f"{e}"
        )

    return iceberg_table


# ============================================================
# CONVERT ONE DATASET
# ============================================================

def convert_to_iceberg(
    table_folder
):

    table_name = (
        table_folder.name
    )

    identifier = (
        f"{NAMESPACE}.{table_name}"
    )


    print()
    print("=" * 100)

    print(
        f"CONVERTING TABLE: "
        f"{table_name}"
    )

    print("=" * 100)


    # ========================================================
    # FIND PARQUET
    # ========================================================

    parquet_files = (
        find_parquet_files(
            table_folder
        )
    )


    if not parquet_files:

        print(
            f"No Parquet files found for: "
            f"{table_name}"
        )

        return


    print()
    print(
        f"Parquet files found: "
        f"{len(parquet_files)}"
    )


    for parquet_file in parquet_files:

        print(
            f"  - {parquet_file}"
        )


    # ========================================================
    # READ DATA
    # ========================================================

    df = (
        read_parquet_files(
            parquet_files
        )
    )


    print()
    print(
        f"Total rows: "
        f"{df.height}"
    )

    print(
        f"Total columns: "
        f"{df.width}"
    )


    # ========================================================
    # PREPARE DATA
    # ========================================================

    df = (
        prepare_dataframe(
            df
        )
    )


    # ========================================================
    # DISPLAY COLUMNS
    # ========================================================

    print()
    print(
        "Columns:"
    )

    for column in df.columns:

        print(
            f"  - {column}: "
            f"{df[column].dtype}"
        )


    # ========================================================
    # CREATE / LOAD ICEBERG TABLE
    # ========================================================

    table_exists = (
        catalog.table_exists(
            identifier
        )
    )


    if table_exists:

        print()
        print(
            "Iceberg table already exists."
        )

        iceberg_table = (
            catalog.load_table(
                identifier
            )
        )

        # ----------------------------------------------------
        # Replace current contents
        # ----------------------------------------------------

        write_to_iceberg(
            df,
            iceberg_table,
            overwrite=True
        )

    else:

        print()
        print(
            "Iceberg table does not exist."
        )

        print(
            "Creating new table..."
        )

        iceberg_table = (
            create_iceberg_table(
                table_name,
                df
            )
        )

        # ----------------------------------------------------
        # Initial data load
        # ----------------------------------------------------

        write_to_iceberg(
            df,
            iceberg_table,
            overwrite=False
        )


    # ========================================================
    # VERIFY
    # ========================================================

    verify_iceberg_table(
        identifier
    )


    print()
    print(
        f"Successfully processed Iceberg table: "
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

    print(
        "CURATED PARQUET → APACHE ICEBERG"
    )

    print("=" * 100)


    print()
    print(
        f"Source : "
        f"{CURATED_FOLDER}"
    )

    print(
        f"Target : "
        f"{WAREHOUSE_FOLDER}"
    )


    # ========================================================
    # DISCOVER TABLES
    # ========================================================

    table_folders = (
        discover_tables()
    )


    if not table_folders:

        raise Exception(
            "No curated table folders found."
        )


    print()
    print(
        "Tables discovered:"
    )


    for folder in table_folders:

        print(
            f"  - {folder.name}"
        )


    # ========================================================
    # PROCESS TABLES
    # ========================================================

    for table_folder in table_folders:

        try:

            convert_to_iceberg(
                table_folder
            )

        except Exception as e:

            print()
            print("=" * 100)

            print(
                f"ERROR processing "
                f"{table_folder.name}"
            )

            print(
                f"Error: {e}"
            )

            print("=" * 100)

            raise


    # ========================================================
    # LIST TABLES
    # ========================================================

    print()
    print("=" * 100)

    print(
        "ICEBERG TABLES CREATED"
    )

    print("=" * 100)


    tables = (
        catalog.list_tables(
            NAMESPACE
        )
    )


    if not tables:

        print(
            "No Iceberg tables found."
        )

    else:

        for table in tables:

            print(
                f"  - {table}"
            )


    # ========================================================
    # COMPLETE
    # ========================================================

    print()
    print("=" * 100)

    print(
        "CONVERSION COMPLETED"
    )

    print("=" * 100)


    print()
    print(
        "Iceberg Warehouse:"
    )

    print(
        WAREHOUSE_FOLDER
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()