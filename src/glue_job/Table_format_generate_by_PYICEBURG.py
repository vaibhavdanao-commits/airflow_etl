import sys

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8")
from pathlib import Path
import json

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
# INCREMENTAL PROCESSING METADATA
# ============================================================

PROCESSED_FILES_METADATA = (
    WAREHOUSE_FOLDER /
    "iceberg_processed_files.json"
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
# INITIALIZE APACHE ICEBERG CATALOG
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

print(
    f"Metadata  : {PROCESSED_FILES_METADATA}"
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
# LOAD PROCESSED FILE METADATA
# ============================================================

def load_processed_files():

    if not PROCESSED_FILES_METADATA.exists():

        print()
        print(
            "No incremental metadata file found."
        )

        print(
            "This will be treated as the first run."
        )

        return {}


    try:

        with open(
            PROCESSED_FILES_METADATA,
            "r",
            encoding="utf-8"
        ) as file:

            data = json.load(file)


        if not isinstance(
            data,
            dict
        ):

            print()
            print(
                "Invalid metadata format."
            )

            return {}


        print()
        print(
            "Incremental metadata loaded."
        )

        for table_name, files in data.items():

            print(
                f"  {table_name}: "
                f"{len(files)} processed file(s)"
            )


        return data


    except Exception as e:

        print()
        print(
            f"Unable to load metadata: {e}"
        )

        print(
            "Starting with empty metadata."
        )

        return {}


# ============================================================
# SAVE PROCESSED FILE METADATA
# ============================================================

def save_processed_files(
    processed_files
):

    temp_file = (
        PROCESSED_FILES_METADATA.with_suffix(
            ".tmp"
        )
    )


    with open(
        temp_file,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            processed_files,
            file,
            indent=4
        )


    temp_file.replace(
        PROCESSED_FILES_METADATA
    )


    print()
    print(
        "Incremental metadata saved:"
    )

    print(
        PROCESSED_FILES_METADATA
    )


# ============================================================
# GLOBAL PROCESSED FILE METADATA
# ============================================================

processed_files_metadata = (
    load_processed_files()
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
# GET RELATIVE FILE PATH
# ============================================================

def get_relative_file_path(
    parquet_file
):

    return str(
        parquet_file
        .resolve()
        .relative_to(
            CURATED_FOLDER.resolve()
        )
    ).replace(
        "\\",
        "/"
    )


# ============================================================
# FIND ONLY NEW FILES
# ============================================================

def find_new_parquet_files(
    table_name,
    parquet_files
):

    already_processed = set(
        processed_files_metadata.get(
            table_name,
            []
        )
    )


    new_files = []


    for parquet_file in parquet_files:

        relative_path = (
            get_relative_file_path(
                parquet_file
            )
        )


        if relative_path in already_processed:

            print()
            print(
                f"SKIP - Already processed:"
            )

            print(
                f"  {relative_path}"
            )

        else:

            print()
            print(
                f"NEW FILE:"
            )

            print(
                f"  {relative_path}"
            )

            new_files.append(
                parquet_file
            )


    return new_files


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


        print(
            f"Columns  : {df.width}"
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
            column.startswith("__index")
            or column.startswith("Unnamed:")
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
                pl.col(column).cast(
                    pl.String
                )
            )


    # --------------------------------------------------------
    # Convert Null columns to nullable String
    # --------------------------------------------------------

    null_columns = []


    for column in df.columns:

        if df[column].dtype == pl.Null:

            null_columns.append(
                column
            )


    if null_columns:

        print()
        print(
            "Converting Null columns to nullable String:"
        )


        for column in null_columns:

            print(
                f"  - {column}: "
                f"Null → String"
            )


            df = df.with_columns(
                pl.col(column).cast(
                    pl.String
                )
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
# CHECK ICEBERG SCHEMA COMPATIBILITY
# ============================================================

def iceberg_schema_matches_dataframe(
    iceberg_table,
    df
):

    print()
    print(
        "Checking existing Iceberg schema..."
    )


    existing_schema = (
        iceberg_table.schema()
    )


    existing_fields = list(
        existing_schema.fields
    )


    expected_schema = (
        create_iceberg_schema(
            df
        )
    )


    expected_fields = list(
        expected_schema.fields
    )


    existing_map = {

        field.name:
        field.field_type

        for field in existing_fields

    }


    expected_map = {

        field.name:
        field.field_type

        for field in expected_fields

    }


    compatible = True


    # --------------------------------------------------------
    # Existing fields
    # --------------------------------------------------------

    existing_names = [

        field.name

        for field in existing_fields

    ]


    expected_names = [

        field.name

        for field in expected_fields

    ]


    # --------------------------------------------------------
    # Missing / new columns
    # --------------------------------------------------------

    missing_in_dataframe = [

        name

        for name in existing_names

        if name not in expected_map

    ]


    new_in_dataframe = [

        name

        for name in expected_names

        if name not in existing_map

    ]


    if missing_in_dataframe:

        compatible = False

        print()
        print(
            "Fields present in Iceberg "
            "but missing from DataFrame:"
        )


        for name in missing_in_dataframe:

            print(
                f"  - {name}"
            )


    if new_in_dataframe:

        compatible = False

        print()
        print(
            "New fields present in DataFrame:"
        )


        for name in new_in_dataframe:

            print(
                f"  - {name}"
            )


    # --------------------------------------------------------
    # Type comparison
    # --------------------------------------------------------

    for name in expected_names:

        if name not in existing_map:

            continue


        existing_type = (
            existing_map[name]
        )


        expected_type = (
            expected_map[name]
        )


        if existing_type != expected_type:

            compatible = False


            print()
            print(
                f"Type mismatch for '{name}': "
                f"Iceberg={existing_type}, "
                f"Current={expected_type}"
            )


    if compatible:

        print()
        print(
            "Schema is compatible."
        )

    else:

        print()
        print(
            "Schema is NOT compatible."
        )


    return compatible


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
        "Iceberg table created."
    )


    return iceberg_table


# ============================================================
# CONVERT DATAFRAME TO PYARROW
# ============================================================

def dataframe_to_arrow(
    df
):

    import pyarrow as pa


    return pa.Table.from_pandas(
        df.to_pandas(),
        preserve_index=False
    )


# ============================================================
# WRITE DATA TO ICEBERG - APPEND ONLY
# ============================================================

def write_to_iceberg(
    df,
    iceberg_table
):

    print()
    print(
        "Appending incremental data to Iceberg..."
    )


    if df.height == 0:

        print(
            "No rows to append."
        )

        return


    # --------------------------------------------------------
    # Polars → PyArrow
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
    # APPEND ONLY
    # --------------------------------------------------------

    iceberg_table.append(
        arrow_table
    )


    print()
    print(
        "Incremental data successfully appended."
    )


# ============================================================
# MARK FILES AS PROCESSED
# ============================================================

def mark_files_as_processed(
    table_name,
    parquet_files
):

    if table_name not in processed_files_metadata:

        processed_files_metadata[
            table_name
        ] = []


    existing = set(
        processed_files_metadata[
            table_name
        ]
    )


    for parquet_file in parquet_files:

        relative_path = (
            get_relative_file_path(
                parquet_file
            )
        )


        existing.add(
            relative_path
        )


    processed_files_metadata[
        table_name
    ] = sorted(
        existing
    )


    save_processed_files(
        processed_files_metadata
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
    print(
        "-" * 100
    )


    print(
        f"ICEBERG TABLE VERIFIED: "
        f"{identifier}"
    )


    print(
        "-" * 100
    )


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


    if len(result_df) > 0:

        print(
            result_df
            .head(10)
            .to_string(
                index=False
            )
        )

    else:

        print(
            "No rows found."
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
# CONVERT ONE DATASET - INCREMENTAL
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
        f"PROCESSING TABLE: "
        f"{table_name}"
    )


    print("=" * 100)


    # ========================================================
    # FIND ALL PARQUET FILES
    # ========================================================

    parquet_files = (
        find_parquet_files(
            table_folder
        )
    )


    if not parquet_files:

        print()
        print(
            f"No Parquet files found for: "
            f"{table_name}"
        )

        return


    print()
    print(
        f"Total Parquet files found: "
        f"{len(parquet_files)}"
    )


    # ========================================================
    # FIND ONLY NEW FILES
    # ========================================================

    new_files = (
        find_new_parquet_files(
            table_name,
            parquet_files
        )
    )


    # ========================================================
    # NO NEW FILES
    # ========================================================

    if not new_files:

        print()
        print("=" * 100)

        print(
            f"NO NEW FILES FOR: "
            f"{table_name}"
        )

        print(
            "Nothing to append to Iceberg."
        )

        print("=" * 100)

        return


    # ========================================================
    # DISPLAY NEW FILES
    # ========================================================

    print()
    print(
        "New files to process:"
    )


    for parquet_file in new_files:

        print(
            f"  - "
            f"{get_relative_file_path(parquet_file)}"
        )


    # ========================================================
    # READ ONLY NEW FILES
    # ========================================================

    df = (
        read_parquet_files(
            new_files
        )
    )


    print()
    print(
        f"Incremental rows: "
        f"{df.height}"
    )


    print(
        f"Incremental columns: "
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
    # CHECK ICEBERG TABLE
    # ========================================================

    table_exists = (
        catalog.table_exists(
            identifier
        )
    )


    # ========================================================
    # FIRST LOAD
    # ========================================================

    if not table_exists:

        print()
        print(
            "Iceberg table does not exist."
        )


        print(
            "Creating new Iceberg table..."
        )


        iceberg_table = (
            create_iceberg_table(
                table_name,
                df
            )
        )


        # ----------------------------------------------------
        # Initial append
        # ----------------------------------------------------

        write_to_iceberg(
            df,
            iceberg_table
        )


    # ========================================================
    # INCREMENTAL LOAD
    # ========================================================

    else:

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
        # Validate schema
        # ----------------------------------------------------

        schema_compatible = (
            iceberg_schema_matches_dataframe(
                iceberg_table,
                df
            )
        )


        if not schema_compatible:

            print()
            print("=" * 100)

            print(
                "ERROR: INCREMENTAL SCHEMA MISMATCH"
            )

            print("=" * 100)

            print()
            print(
                f"Table: {identifier}"
            )

            print()
            print(
                "The new curated file does not "
                "match the existing Iceberg schema."
            )

            print()
            print(
                "No data was appended."
            )

            print()
            print(
                "The file will NOT be marked as processed."
            )

            raise Exception(
                f"Schema mismatch for "
                f"{identifier}"
            )


        # ----------------------------------------------------
        # Append incremental data
        # ----------------------------------------------------

        write_to_iceberg(
            df,
            iceberg_table
        )


    # ========================================================
    # MARK FILES AS PROCESSED
    # ========================================================

    mark_files_as_processed(
        table_name,
        new_files
    )


    # ========================================================
    # VERIFY
    # ========================================================

    verify_iceberg_table(
        identifier
    )


    print()
    print("=" * 100)


    print(
        f"SUCCESSFULLY PROCESSED: "
        f"{identifier}"
    )


    print(
        f"New files processed: "
        f"{len(new_files)}"
    )


    print(
        f"New rows appended: "
        f"{df.height}"
    )


    print("=" * 100)


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

    print(
        "INCREMENTAL FILE PROCESSING"
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


    print()
    print(
        f"Processed file metadata:"
    )


    print(
        PROCESSED_FILES_METADATA
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
    # LIST ICEBERG TABLES
    # ========================================================

    print()
    print("=" * 100)


    print(
        "ICEBERG TABLES"
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
    # FINAL STATUS
    # ========================================================

    print()
    print("=" * 100)


    print(
        "INCREMENTAL CONVERSION COMPLETED"
    )


    print("=" * 100)


    print()
    print(
        "Iceberg Warehouse:"
    )


    print(
        WAREHOUSE_FOLDER
    )


    print()
    print(
        "Processed-file metadata:"
    )


    print(
        PROCESSED_FILES_METADATA
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()