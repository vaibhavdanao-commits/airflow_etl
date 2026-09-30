import sys
import json
from pathlib import Path
from datetime import datetime

import pandas as pd


# ============================================================
# ADD SRC FOLDER TO PYTHON PATH
# ============================================================

SRC_FOLDER = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SRC_FOLDER))


# ============================================================
# IMPORT CONFIG
# ============================================================

from config import (
    LOCAL_FOLDER,
    PROCESSED_FOLDER,
    CURATED_FOLDER,
    QUARANTINE_FOLDER
)


# ============================================================
# INCREMENTAL CONTROL
# ============================================================

CONTROL_FILE = PROCESSED_FOLDER / "incremental_control.json"

CHUNK_SIZE = 50000


# ============================================================
# DISPLAY CONFIGURATION
# ============================================================

print("=" * 80)
print("RETAIL INCREMENTAL ETL JOB STARTED - LOCAL PANDAS")
print("=" * 80)

print(f"Raw Folder        : {LOCAL_FOLDER}")
print(f"Processed Folder  : {PROCESSED_FOLDER}")
print(f"Curated Folder    : {CURATED_FOLDER}")
print(f"Quarantine Folder : {QUARANTINE_FOLDER}")
print(f"Control File      : {CONTROL_FILE}")
print(f"Chunk Size        : {CHUNK_SIZE}")

print("=" * 80)


# ============================================================
# CREATE OUTPUT DIRECTORIES
# ============================================================

PROCESSED_FOLDER.mkdir(
    parents=True,
    exist_ok=True
)

CURATED_FOLDER.mkdir(
    parents=True,
    exist_ok=True
)

QUARANTINE_FOLDER.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# CONTROL FILE FUNCTIONS
# ============================================================

def load_control():

    if not CONTROL_FILE.exists():

        print()
        print("No incremental control file found.")
        print("This will be treated as the first run.")

        return {
            "datasets": {}
        }

    try:

        with open(
            CONTROL_FILE,
            "r",
            encoding="utf-8"
        ) as file:

            control = json.load(file)

        if "datasets" not in control:
            control["datasets"] = {}

        print()
        print(
            f"Incremental control loaded: {CONTROL_FILE}"
        )

        return control

    except Exception as e:

        print()
        print(
            f"WARNING: Could not read control file: {e}"
        )

        print(
            "Starting with empty incremental state."
        )

        return {
            "datasets": {}
        }


def save_control(control):

    temporary_file = CONTROL_FILE.with_suffix(".tmp")

    with open(
        temporary_file,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            control,
            file,
            indent=4
        )

    temporary_file.replace(
        CONTROL_FILE
    )

    print()
    print(
        f"Incremental control updated: {CONTROL_FILE}"
    )


# ============================================================
# DATASET CONTROL
# ============================================================

def get_dataset_control(
    control,
    dataset
):

    if "datasets" not in control:
        control["datasets"] = {}

    if dataset not in control["datasets"]:

        control["datasets"][dataset] = {
            "last_processed_time": None,
            "last_run_time": None,
            "records_processed": 0,
            "processed_files": {}
        }

    dataset_control = control["datasets"][dataset]

    if "processed_files" not in dataset_control:
        dataset_control["processed_files"] = {}

    return dataset_control


# ============================================================
# GET LAST WATERMARK
# ============================================================

def get_last_watermark(
    control,
    dataset
):

    dataset_control = get_dataset_control(
        control,
        dataset
    )

    return dataset_control.get(
        "last_processed_time"
    )


# ============================================================
# GET FILE SIGNATURE
# ============================================================

def get_file_signature(csv_file):

    stat = csv_file.stat()

    return {
        "modified_time":
            datetime.fromtimestamp(
                stat.st_mtime
            ).isoformat(),

        "modified_time_ns":
            int(
                stat.st_mtime_ns
            ),

        "size":
            int(
                stat.st_size
            )
    }


# ============================================================
# GET RELATIVE FILE PATH
# ============================================================

def get_relative_file_path(
    csv_file,
    dataset_path
):

    try:

        return str(
            csv_file.relative_to(
                dataset_path
            )
        ).replace(
            "\\",
            "/"
        )

    except ValueError:

        return str(
            csv_file
        ).replace(
            "\\",
            "/"
        )


# ============================================================
# CHECK WHETHER FILE WAS ALREADY PROCESSED
# ============================================================

def file_was_processed(
    control,
    dataset,
    csv_file,
    dataset_path
):

    dataset_control = get_dataset_control(
        control,
        dataset
    )

    processed_files = dataset_control.get(
        "processed_files",
        {}
    )

    relative_path = get_relative_file_path(
        csv_file,
        dataset_path
    )

    current_signature = get_file_signature(
        csv_file
    )

    previous_signature = processed_files.get(
        relative_path
    )

    if previous_signature is None:
        return False

    previous_mtime_ns = previous_signature.get(
        "modified_time_ns"
    )

    previous_size = previous_signature.get(
        "size"
    )

    current_mtime_ns = current_signature.get(
        "modified_time_ns"
    )

    current_size = current_signature.get(
        "size"
    )

    if (
        previous_mtime_ns == current_mtime_ns
        and
        previous_size == current_size
    ):
        return True

    return False


# ============================================================
# MARK FILES AS SUCCESSFULLY PROCESSED
# ============================================================

def mark_files_processed(
    control,
    dataset,
    dataset_path,
    csv_files,
    records_processed
):

    dataset_control = get_dataset_control(
        control,
        dataset
    )

    processed_files = dataset_control.setdefault(
        "processed_files",
        {}
    )

    latest_time = None

    for csv_file in csv_files:

        signature = get_file_signature(
            csv_file
        )

        relative_path = get_relative_file_path(
            csv_file,
            dataset_path
        )

        processed_files[relative_path] = {

            "modified_time":
                signature["modified_time"],

            "modified_time_ns":
                signature["modified_time_ns"],

            "size":
                signature["size"],

            "processed_at":
                datetime.now().isoformat()
        }

        file_time = datetime.fromtimestamp(
            csv_file.stat().st_mtime
        )

        if (
            latest_time is None
            or
            file_time > latest_time
        ):

            latest_time = file_time

    if latest_time is not None:

        dataset_control[
            "last_processed_time"
        ] = latest_time.isoformat()

    dataset_control[
        "last_run_time"
    ] = datetime.now().isoformat()

    dataset_control[
        "records_processed"
    ] = int(records_processed)


# ============================================================
# CLEAN COLUMN NAMES
# ============================================================

def clean_column_names(df):

    df = df.copy()

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(
            " ",
            "_",
            regex=False
        )
        .str.replace(
            "-",
            "_",
            regex=False
        )
    )

    return df


# ============================================================
# FIND CSV FILES
# ============================================================

def find_csv_files(dataset_path):

    csv_files = list(
        Path(dataset_path).rglob("*.csv")
    )

    if not csv_files:

        raise FileNotFoundError(
            f"No CSV files found under: {dataset_path}"
        )

    return csv_files


# ============================================================
# DETERMINE FILE TIME
# ============================================================

def get_file_time(csv_file):

    timestamp = csv_file.stat().st_mtime

    return datetime.fromtimestamp(
        timestamp
    )


# ============================================================
# FIND INCREMENTAL FILES
#
# IMPORTANT:
#
# This function NEVER reads CSV contents.
#
# It checks only:
#
# - file path
# - modified time
# - modified time nanoseconds
# - file size
#
# Previously processed unchanged files are skipped.
# ============================================================

def find_incremental_files(
    dataset_path,
    dataset,
    control
):

    all_files = find_csv_files(
        dataset_path
    )

    print()

    print(
        f"Total CSV files found for {dataset}: "
        f"{len(all_files)}"
    )

    incremental_files = []

    for csv_file in sorted(
        all_files,
        key=get_file_time
    ):

        relative_path = get_relative_file_path(
            csv_file,
            dataset_path
        )

        file_time = get_file_time(
            csv_file
        )

        signature = get_file_signature(
            csv_file
        )

        already_processed = file_was_processed(
            control,
            dataset,
            csv_file,
            dataset_path
        )

        print()

        print(
            f"Checking file: {relative_path}"
        )

        print(
            f"File time    : {file_time}"
        )

        print(
            f"File size    : {signature['size']} bytes"
        )

        if already_processed:

            print(
                "STATUS       : ALREADY PROCESSED"
            )
            continue

        print(
            "STATUS       : NEW / MODIFIED"
        )

        print(
            "ACTION       : PROCESS"
        )

        incremental_files.append(
            csv_file
        )

    print()

    print(
        f"NEW/MODIFIED CSV files selected: "
        f"{len(incremental_files)}"
    )

    if incremental_files:

        print()

        print(
            "Files that WILL be processed:"
        )

        for csv_file in incremental_files:

            print(
                f"  + {csv_file}"
            )

    else:

        print()

        print(
            "No new or modified CSV files found."
        )

    return incremental_files


# ============================================================
# NORMALIZE ID COLUMNS
#
# Important for PyArrow / Parquet.
#
# Prevents mixed values such as:
#
# 10001
# "10002"
# 10003
#
# inside the same object column.
# ============================================================

def normalize_id_columns(df):

    df = df.copy()

    id_columns = [
        "order_id",
        "customer_id",
        "product_id",
        "store_id"
    ]

    for column in id_columns:

        if column in df.columns:

            df[column] = (
                df[column]
                .astype("string")
                .str.strip()
            )

            df[column] = df[column].where(
                df[column].notna(),
                None
            )

    return df


# ============================================================
# NORMALIZE MIXED OBJECT COLUMNS
#
# PyArrow can fail when an object column contains
# mixed Python types.
#
# Example:
#
# "ABC"
# 123
# b"XYZ"
#
# We convert only mixed object columns to string.
# ============================================================

def normalize_mixed_object_columns(df):

    df = df.copy()

    for column in df.columns:

        if df[column].dtype == "object":

            non_null_values = (
                df[column]
                .dropna()
            )

            if len(non_null_values) == 0:
                continue

            try:

                unique_types = (
                    non_null_values
                    .map(type)
                    .nunique()
                )

            except Exception:

                unique_types = 1

            if unique_types > 1:

                print(
                    f"Mixed object types detected in "
                    f"'{column}'. Converting to string."
                )

                df[column] = (
                    df[column]
                    .astype("string")
                )

    return df


# ============================================================
# PREPARE DATAFRAME FOR PARQUET
# ============================================================

def prepare_for_parquet(df):

    df = df.copy()

    # Normalize identifiers
    df = normalize_id_columns(
        df
    )

    # Normalize mixed object columns
    df = normalize_mixed_object_columns(
        df
    )

    return df


# ============================================================
# WRITE PROCESSED DATA
# ============================================================

def write_processed(
    df,
    dataset,
    append=False
):

    output_path = (
        PROCESSED_FOLDER / dataset
    )

    output_path.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        output_path /
        f"{dataset}.parquet"
    )

    print()

    print(
        f"Writing processed data: {output_file}"
    )

    df = prepare_for_parquet(
        df
    )

    if append and output_file.exists():

        try:

            existing_df = pd.read_parquet(
                output_file
            )

            existing_df = prepare_for_parquet(
                existing_df
            )

            df = pd.concat(
                [
                    existing_df,
                    df
                ],
                ignore_index=True
            )

        except Exception as e:

            print(
                f"WARNING reading existing processed file: {e}"
            )

    df.to_parquet(
        output_file,
        index=False
    )

    print(
        f"Processed records written: {len(df)}"
    )


# ============================================================
# WRITE QUARANTINE DATA
# ============================================================

def write_quarantine(
    df,
    dataset
):

    if df.empty:

        print(
            f"No invalid records for {dataset}"
        )

        return

    output_path = (
        QUARANTINE_FOLDER / dataset
    )

    output_path.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        output_path /
        f"{dataset}_invalid.parquet"
    )

    print()

    print(
        f"Writing quarantine data: {output_file}"
    )

    df = prepare_for_parquet(
        df
    )

    if output_file.exists():

        try:

            existing_df = pd.read_parquet(
                output_file
            )

            existing_df = prepare_for_parquet(
                existing_df
            )

            df = pd.concat(
                [
                    existing_df,
                    df
                ],
                ignore_index=True
            )

        except Exception as e:

            print(
                f"WARNING reading existing quarantine file: {e}"
            )

    df.to_parquet(
        output_file,
        index=False
    )

    print(
        f"Quarantine records written: {len(df)}"
    )


# ============================================================
# REQUIRED COLUMN VALIDATION
# ============================================================

def require_columns(
    df,
    required_columns
):

    missing = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing:

        raise Exception(
            f"Missing required columns: {missing}"
        )


# ============================================================
# READ ONLY SELECTED INCREMENTAL FILES
# ============================================================

def read_incremental_dataset(
    dataset_path,
    dataset,
    control
):

    print()

    print(
        f"Reading incremental data: {dataset_path}"
    )

    csv_files = find_incremental_files(
        dataset_path,
        dataset,
        control
    )

    if not csv_files:

        print()

        print(
            f"No new or modified files for {dataset}."
        )

        return (
            pd.DataFrame(),
            None,
            []
        )

    dataframes = []

    total_records = 0

    latest_file_time = None

    # ========================================================
    # READ ONLY NEW / MODIFIED FILES
    # ========================================================

    for csv_file in csv_files:

        print()

        print(
            "------------------------------------------------"
        )

        print(
            f"READING FILE: {csv_file}"
        )

        print(
            "------------------------------------------------"
        )

        file_time = get_file_time(
            csv_file
        )

        if (
            latest_file_time is None
            or
            file_time > latest_file_time
        ):

            latest_file_time = file_time

        try:

            chunk_number = 0

            for chunk_df in pd.read_csv(
                csv_file,
                dtype=str,
                chunksize=CHUNK_SIZE
            ):

                chunk_number += 1

                print(
                    f"  Processing chunk "
                    f"{chunk_number}: "
                    f"{len(chunk_df)} records"
                )

                chunk_df = clean_column_names(
                    chunk_df
                )

                dataframes.append(
                    chunk_df
                )

                total_records += len(
                    chunk_df
                )

        except Exception as e:

            print()

            print(
                f"ERROR reading file: {csv_file}"
            )

            print(
                f"Error details: {e}"
            )

            raise

    if not dataframes:

        return (
            pd.DataFrame(),
            latest_file_time,
            csv_files
        )

    df = pd.concat(
        dataframes,
        ignore_index=True
    )

    print()

    print(
        f"Incremental Record Count: {len(df)}"
    )

    print(
        f"Total records read: {total_records}"
    )

    print()

    print("Columns:")

    print(
        list(df.columns)
    )

    return (
        df,
        latest_file_time,
        csv_files
    )


# ============================================================
# DETECT DELETE RECORDS
# ============================================================

def detect_delete_records(df):

    if df.empty:
        return pd.DataFrame()

    operation_columns = [
        "operation",
        "change_type",
        "cdc_operation",
        "record_operation"
    ]

    delete_column = None

    for column in operation_columns:

        if column in df.columns:

            delete_column = column

            break

    if delete_column is None:

        return pd.DataFrame()

    operation_values = (
        df[delete_column]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    delete_mask = (
        operation_values.isin(
            [
                "DELETE",
                "D"
            ]
        )
    )

    return df[
        delete_mask
    ].copy()


# ============================================================
# APPLY DELETE
# ============================================================

def apply_deletes(
    dataset,
    delete_df,
    primary_key
):

    if delete_df.empty:

        print(
            f"No delete records for {dataset}."
        )

        return

    if primary_key not in delete_df.columns:

        print(
            f"Delete key {primary_key} "
            f"not found for {dataset}."
        )

        return

    curated_file = (
        CURATED_FOLDER
        / dataset
        / f"{dataset}.parquet"
    )

    if not curated_file.exists():

        print(
            f"No existing curated data for {dataset}."
        )

        return

    print()

    print(
        f"Reading curated state for delete: {curated_file}"
    )

    existing_df = pd.read_parquet(
        curated_file
    )

    # Normalize primary key on both sides
    existing_df[primary_key] = (
        existing_df[primary_key]
        .astype("string")
        .str.strip()
    )

    delete_keys = set(
        delete_df[
            primary_key
        ]
        .astype("string")
        .str.strip()
        .dropna()
        .tolist()
    )

    before_count = len(
        existing_df
    )

    existing_df = existing_df[
        ~existing_df[
            primary_key
        ]
        .isin(delete_keys)
    ].copy()

    after_count = len(
        existing_df
    )

    deleted_count = (
        before_count - after_count
    )

    existing_df = prepare_for_parquet(
        existing_df
    )

    existing_df.to_parquet(
        curated_file,
        index=False
    )

    print()

    print(
        f"Deleted records from {dataset}: "
        f"{deleted_count}"
    )


# ============================================================
# FAST INCREMENTAL UPSERT
#
# IMPORTANT:
#
# Uses vectorized CONCAT + DROP_DUPLICATES.
#
# No record-by-record .loc loop.
#
# Also fixes PyArrow mixed-type errors.
# ============================================================

def incremental_upsert(
    incoming_df,
    dataset,
    primary_key,
    timestamp_column="updated_at"
):

    if incoming_df is None or incoming_df.empty:

        print(
            f"No incoming records for {dataset}."
        )

        return incoming_df

    curated_path = (
        CURATED_FOLDER / dataset
    )

    curated_path.mkdir(
        parents=True,
        exist_ok=True
    )

    curated_file = (
        curated_path /
        f"{dataset}.parquet"
    )

    print()

    print("=" * 80)
    print(f"INCREMENTAL UPSERT: {dataset}")
    print("=" * 80)

    # ========================================================
    # PRIMARY KEY VALIDATION
    # ========================================================

    if primary_key not in incoming_df.columns:

        raise Exception(
            f"Primary key {primary_key} "
            f"not found in incoming {dataset} data."
        )

    # ========================================================
    # COPY
    # ========================================================

    incoming_df = incoming_df.copy()

    # ========================================================
    # NORMALIZE ID COLUMNS
    # ========================================================

    incoming_df = normalize_id_columns(
        incoming_df
    )

    # Explicit primary key normalization
    incoming_df[primary_key] = (
        incoming_df[primary_key]
        .astype("string")
        .str.strip()
    )

    # ========================================================
    # REMOVE NULL PRIMARY KEYS
    # ========================================================

    before_pk_count = len(
        incoming_df
    )

    incoming_df = incoming_df[
        incoming_df[primary_key].notna()
    ].copy()

    removed_pk_count = (
        before_pk_count -
        len(incoming_df)
    )

    if removed_pk_count > 0:

        print(
            f"Removed records with null "
            f"{primary_key}: {removed_pk_count}"
        )

    # ========================================================
    # REMOVE DUPLICATES FROM INCOMING
    # ========================================================

    print()

    print(
        f"Incoming records before deduplication: "
        f"{len(incoming_df)}"
    )

    if timestamp_column in incoming_df.columns:

        print(
            f"Timestamp column found: {timestamp_column}"
        )

        try:

            incoming_df[timestamp_column] = pd.to_datetime(
                incoming_df[timestamp_column],
                errors="coerce"
            )

            incoming_df = (
                incoming_df
                .sort_values(
                    timestamp_column,
                    na_position="first"
                )
                .drop_duplicates(
                    subset=[primary_key],
                    keep="last"
                )
            )

        except Exception:

            incoming_df = (
                incoming_df
                .drop_duplicates(
                    subset=[primary_key],
                    keep="last"
                )
            )

    else:

        incoming_df = (
            incoming_df
            .drop_duplicates(
                subset=[primary_key],
                keep="last"
            )
        )

    print()

    print(
        f"Incoming records after deduplication: "
        f"{len(incoming_df)}"
    )

    # ========================================================
    # FIRST LOAD
    # ========================================================

    if not curated_file.exists():

        print()

        print(
            f"No existing {dataset} table."
        )

        print(
            "Creating initial curated dataset."
        )

        incoming_df = prepare_for_parquet(
            incoming_df
        )

        incoming_df.to_parquet(
            curated_file,
            index=False
        )

        print()

        print(
            f"Initial {dataset} records: "
            f"{len(incoming_df)}"
        )

        print(
            f"Curated file: {curated_file}"
        )

        return incoming_df

    # ========================================================
    # READ EXISTING CURATED STATE
    #
    # IMPORTANT:
    #
    # This does NOT read old RAW CSV files.
    #
    # It only reads the current curated state.
    # ========================================================

    print()

    print(
        f"Reading existing curated state: "
        f"{curated_file}"
    )

    existing_df = pd.read_parquet(
        curated_file
    )

    print(
        f"Existing {dataset} records: "
        f"{len(existing_df)}"
    )

    print(
        f"Incoming {dataset} records: "
        f"{len(incoming_df)}"
    )

    # ========================================================
    # PRIMARY KEY VALIDATION
    # ========================================================

    if primary_key not in existing_df.columns:

        raise Exception(
            f"Primary key {primary_key} "
            f"not found in existing {dataset} data."
        )

    # ========================================================
    # NORMALIZE EXISTING DATA
    # ========================================================

    existing_df = normalize_id_columns(
        existing_df
    )

    existing_df[primary_key] = (
        existing_df[primary_key]
        .astype("string")
        .str.strip()
    )

    # ========================================================
    # REMOVE DUPLICATES FROM EXISTING STATE
    # ========================================================

    existing_before = len(
        existing_df
    )

    if timestamp_column in existing_df.columns:

        try:

            existing_df[timestamp_column] = pd.to_datetime(
                existing_df[timestamp_column],
                errors="coerce"
            )

            existing_df = (
                existing_df
                .sort_values(
                    timestamp_column,
                    na_position="first"
                )
                .drop_duplicates(
                    subset=[primary_key],
                    keep="last"
                )
            )

        except Exception:

            existing_df = (
                existing_df
                .drop_duplicates(
                    subset=[primary_key],
                    keep="last"
                )
            )

    else:

        existing_df = (
            existing_df
            .drop_duplicates(
                subset=[primary_key],
                keep="last"
            )
        )

    removed_existing_duplicates = (
        existing_before -
        len(existing_df)
    )

    if removed_existing_duplicates > 0:

        print(
            f"Removed duplicate existing records: "
            f"{removed_existing_duplicates}"
        )

    # ========================================================
    # SCHEMA EVOLUTION
    # ========================================================

    existing_columns = list(
        existing_df.columns
    )

    incoming_columns = list(
        incoming_df.columns
    )

    # New columns from incoming data
    new_columns = [
        column
        for column in incoming_columns
        if column not in existing_columns
    ]

    if new_columns:

        print()

        print(
            f"New columns detected: {new_columns}"
        )

    for column in new_columns:

        existing_df[column] = pd.NA

    # Add missing existing columns to incoming
    for column in existing_df.columns:

        if column not in incoming_df.columns:

            incoming_df[column] = pd.NA

    # ========================================================
    # SAME COLUMN ORDER
    # ========================================================

    incoming_df = incoming_df[
        existing_df.columns
    ]

    # ========================================================
    # NORMALIZE IDs AFTER SCHEMA ALIGNMENT
    # ========================================================

    existing_df = normalize_id_columns(
        existing_df
    )

    incoming_df = normalize_id_columns(
        incoming_df
    )

    # ========================================================
    # FIND INSERT / UPDATE COUNTS
    # ========================================================

    existing_keys = set(
        existing_df[
            primary_key
        ]
        .dropna()
        .astype(str)
        .str.strip()
    )

    incoming_keys = set(
        incoming_df[
            primary_key
        ]
        .dropna()
        .astype(str)
        .str.strip()
    )

    inserted_count = len(
        incoming_keys -
        existing_keys
    )

    matched_count = len(
        incoming_keys &
        existing_keys
    )

    print()

    print(
        f"New records to insert: {inserted_count}"
    )

    print(
        f"Existing records to update: {matched_count}"
    )

    # ========================================================
    # FAST VECTORISED UPSERT
    #
    # Existing data first.
    # Incoming data second.
    #
    # drop_duplicates(keep='last')
    # means incoming wins.
    # ========================================================

    print()

    print(
        "Performing vectorized UPSERT..."
    )

    combined_df = pd.concat(
        [
            existing_df,
            incoming_df
        ],
        ignore_index=True
    )

    print(
        f"Combined records before deduplication: "
        f"{len(combined_df)}"
    )

    # ========================================================
    # TIMESTAMP-AWARE UPSERT
    # ========================================================

    if timestamp_column in combined_df.columns:

        print(
            f"Using timestamp column: "
            f"{timestamp_column}"
        )

        try:

            combined_df[timestamp_column] = pd.to_datetime(
                combined_df[timestamp_column],
                errors="coerce"
            )

            # Existing records have source priority 0.
            # Incoming records have source priority 1.
            #
            # This guarantees incoming records win when
            # timestamps are equal.

            existing_count = len(
                existing_df
            )

            combined_df["_upsert_source"] = 0

            combined_df.loc[
                existing_count:,
                "_upsert_source"
            ] = 1

            combined_df = (
                combined_df
                .sort_values(
                    by=[
                        primary_key,
                        timestamp_column,
                        "_upsert_source"
                    ],
                    na_position="first"
                )
                .drop_duplicates(
                    subset=[primary_key],
                    keep="last"
                )
                .drop(
                    columns=["_upsert_source"]
                )
                .reset_index(
                    drop=True
                )
            )

        except Exception as e:

            print(
                f"WARNING: Timestamp UPSERT failed: {e}"
            )

            print(
                "Falling back to incoming-record-wins UPSERT."
            )

            combined_df = (
                combined_df
                .drop_duplicates(
                    subset=[primary_key],
                    keep="last"
                )
                .reset_index(
                    drop=True
                )
            )

    else:

        # Incoming records were added after existing records.
        # Therefore keep='last' makes incoming data win.

        combined_df = (
            combined_df
            .drop_duplicates(
                subset=[primary_key],
                keep="last"
            )
            .reset_index(
                drop=True
            )
        )

    # ========================================================
    # FINAL PARQUET TYPE CLEANUP
    # ========================================================

    print()

    print(
        "Normalizing final data types before Parquet write..."
    )

    combined_df = prepare_for_parquet(
        combined_df
    )

    # ========================================================
    # FINAL PRIMARY KEY NORMALIZATION
    # ========================================================

    combined_df[primary_key] = (
        combined_df[primary_key]
        .astype("string")
        .str.strip()
    )

    # ========================================================
    # WRITE FINAL CURATED STATE
    # ========================================================

    print()

    print(
        "Writing final curated state..."
    )

    combined_df.to_parquet(
        curated_file,
        index=False
    )

    print()

    print(
        "------------------------------------------------------------"
    )

    print(
        f"UPSERT COMPLETED: {dataset}"
    )

    print(
        f"Inserted records : {inserted_count}"
    )

    print(
        f"Updated records  : {matched_count}"
    )

    print(
        f"Final records    : {len(combined_df)}"
    )

    print(
        f"Curated file     : {curated_file}"
    )

    print(
        "------------------------------------------------------------"
    )

    return combined_df


# ============================================================
# CUSTOMER ETL
# ============================================================

def process_customers(
    dataset_path,
    control
):

    print()

    print("=" * 80)
    print("INCREMENTAL CUSTOMER ETL")
    print("=" * 80)

    (
        df,
        latest_time,
        selected_files
    ) = read_incremental_dataset(
        dataset_path,
        "customers",
        control
    )

    if df.empty:

        return (
            0,
            latest_time,
            selected_files
        )

    required_columns = [
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "signup_date",
        "customer_segment"
    ]

    require_columns(
        df,
        required_columns
    )

    # ========================================================
    # DATA TYPE CONVERSION
    # ========================================================

    df["customer_id"] = pd.to_numeric(
        df["customer_id"],
        errors="coerce"
    ).astype("Int64")

    df["customer_name"] = (
        df["customer_name"]
        .astype("string")
        .str.strip()
    )

    df["email"] = (
        df["email"]
        .astype("string")
        .str.strip()
        .str.lower()
    )

    df["city"] = (
        df["city"]
        .astype("string")
        .str.strip()
    )

    df["state"] = (
        df["state"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    df["signup_date"] = pd.to_datetime(
        df["signup_date"],
        format="%Y-%m-%d",
        errors="coerce"
    )

    df["customer_segment"] = (
        df["customer_segment"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    # ========================================================
    # EMAIL VALIDATION
    # ========================================================

    email_pattern = (
        r"^[A-Za-z0-9._%+-]+"
        r"@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    )

    invalid_mask = (
        df["customer_id"].isna()
        |
        df["email"].isna()
        |
        ~df["email"].str.match(
            email_pattern,
            na=False
        )
    )

    invalid_df = df[
        invalid_mask
    ].copy()

    valid_df = df[
        ~invalid_mask
    ].copy()

    # ========================================================
    # DUPLICATES
    # ========================================================

    valid_df = (
        valid_df
        .drop_duplicates(
            subset=["customer_id"],
            keep="last"
        )
    )

    # ========================================================
    # QUARANTINE
    # ========================================================

    write_quarantine(
        invalid_df,
        "customers"
    )

    # ========================================================
    # PROCESSED
    # ========================================================

    write_processed(
        valid_df,
        "customers",
        append=False
    )

    # ========================================================
    # CURATED
    # ========================================================

    curated_df = valid_df.copy()

    curated_df["customer_name"] = (
        curated_df["customer_name"]
        .astype("string")
        .str.title()
    )

    curated_df["processed_timestamp"] = (
        pd.Timestamp.now()
    )

    # ========================================================
    # UPSERT
    # ========================================================

    incremental_upsert(
        curated_df,
        "customers",
        "customer_id"
    )

    return (
        len(valid_df),
        latest_time,
        selected_files
    )


# ============================================================
# PRODUCT ETL
# ============================================================

def process_products(
    dataset_path,
    control
):

    print()

    print("=" * 80)
    print("INCREMENTAL PRODUCT ETL")
    print("=" * 80)

    (
        df,
        latest_time,
        selected_files
    ) = read_incremental_dataset(
        dataset_path,
        "products",
        control
    )

    if df.empty:

        print()
        print(
            "No new product data to process."
        )

        return (
            0,
            latest_time,
            selected_files
        )

    # ========================================================
    # REQUIRED COLUMNS
    # ========================================================

    required_columns = [
        "product_id",
        "product_name",
        "category",
        "price",
        "supplier"
    ]

    require_columns(
        df,
        required_columns
    )

    # ========================================================
    # DATA TYPE CONVERSION
    # ========================================================

    df["product_id"] = pd.to_numeric(
        df["product_id"],
        errors="coerce"
    ).astype("Int64")

    df["product_name"] = (
        df["product_name"]
        .astype("string")
        .str.strip()
    )

    df["category"] = (
        df["category"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    df["price"] = pd.to_numeric(
        df["price"],
        errors="coerce"
    )

    df["supplier"] = (
        df["supplier"]
        .astype("string")
        .str.strip()
    )

    # ========================================================
    # VALIDATION
    # ========================================================

    invalid_mask = (
        df["product_id"].isna()
        |
        df["product_name"].isna()
        |
        df["product_name"].eq("")
        |
        df["price"].isna()
        |
        (df["price"] < 0)
    )

    invalid_df = df[
        invalid_mask
    ].copy()

    valid_df = df[
        ~invalid_mask
    ].copy()

    # ========================================================
    # REMOVE DUPLICATE PRODUCT IDS
    # ========================================================

    if not valid_df.empty:

        valid_df = (
            valid_df
            .drop_duplicates(
                subset=["product_id"],
                keep="last"
            )
        )

    # ========================================================
    # QUARANTINE
    # ========================================================

    write_quarantine(
        invalid_df,
        "products"
    )

    # ========================================================
    # IF EVERYTHING IS INVALID
    # ========================================================

    if valid_df.empty:

        print()
        print(
            "No valid product records found."
        )

        return (
            0,
            latest_time,
            selected_files
        )

    # ========================================================
    # PROCESSED
    # ========================================================

    write_processed(
        valid_df,
        "products",
        append=False
    )

    # ========================================================
    # CURATED
    # ========================================================

    curated_df = valid_df.copy()

    curated_df["product_name"] = (
        curated_df["product_name"]
        .astype("string")
        .str.title()
    )

    curated_df["processed_timestamp"] = (
        pd.Timestamp.now()
    )

    # ========================================================
    # UPSERT
    # ========================================================

    incremental_upsert(
        curated_df,
        "products",
        "product_id"
    )

    print()

    print(
        "Product ETL completed successfully."
    )

    print(
        f"Valid product records   : {len(valid_df)}"
    )

    print(
        f"Invalid product records : {len(invalid_df)}"
    )

    print(
        f"Files processed         : {len(selected_files)}"
    )

    return (
        len(valid_df),
        latest_time,
        selected_files
    )


# ============================================================
# ORDERS ETL
# ============================================================

def process_orders(
    dataset_path,
    control
):

    print()

    print("=" * 80)
    print("INCREMENTAL ORDERS ETL")
    print("=" * 80)

    (
        df,
        latest_time,
        selected_files
    ) = read_incremental_dataset(
        dataset_path,
        "orders",
        control
    )

    if df.empty:

        return (
            0,
            latest_time,
            selected_files
        )

    # ========================================================
    # REQUIRED COLUMNS
    # ========================================================

    required_columns = [
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "unit_price",
        "payment_method",
        "order_status",
        "store_id"
    ]

    require_columns(
        df,
        required_columns
    )

    # ========================================================
    # DATA TYPE CONVERSION
    # ========================================================

    df["order_id"] = pd.to_numeric(
        df["order_id"],
        errors="coerce"
    ).astype("Int64")

    df["customer_id"] = pd.to_numeric(
        df["customer_id"],
        errors="coerce"
    ).astype("Int64")

    df["product_id"] = pd.to_numeric(
        df["product_id"],
        errors="coerce"
    ).astype("Int64")

    df["order_date"] = pd.to_datetime(
        df["order_date"],
        errors="coerce"
    )

    df["quantity"] = pd.to_numeric(
        df["quantity"],
        errors="coerce"
    )

    df["unit_price"] = pd.to_numeric(
        df["unit_price"],
        errors="coerce"
    )

    df["payment_method"] = (
        df["payment_method"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    df["order_status"] = (
        df["order_status"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    df["store_id"] = (
        df["store_id"]
        .astype("string")
        .str.strip()
    )

    # ========================================================
    # DETECT DELETE RECORDS
    # ========================================================

    delete_df = detect_delete_records(
        df
    )

    if not delete_df.empty:

        print()

        print(
            f"Delete records detected: "
            f"{len(delete_df)}"
        )

        apply_deletes(
            "orders",
            delete_df,
            "order_id"
        )

    # ========================================================
    # REMOVE DELETE RECORDS
    # ========================================================

    operation_column = None

    for column in [
        "operation",
        "change_type",
        "cdc_operation",
        "record_operation"
    ]:

        if column in df.columns:

            operation_column = column

            break

    if operation_column is not None:

        operation_values = (
            df[operation_column]
            .astype("string")
            .str.strip()
            .str.upper()
        )

        df = df[
            ~operation_values.isin(
                [
                    "DELETE",
                    "D"
                ]
            )
        ].copy()

    if df.empty:

        print(
            "No INSERT/UPDATE order records "
            "remaining after delete processing."
        )

        return (
            0,
            latest_time,
            selected_files
        )

    # ========================================================
    # ORDER VALIDATION
    # ========================================================

    invalid_mask = (
        df["order_id"].isna()
        |
        df["customer_id"].isna()
        |
        df["product_id"].isna()
        |
        df["order_date"].isna()
        |
        df["quantity"].isna()
        |
        (df["quantity"] <= 0)
        |
        df["unit_price"].isna()
        |
        (df["unit_price"] < 0)
    )

    invalid_df = df[
        invalid_mask
    ].copy()

    valid_df = df[
        ~invalid_mask
    ].copy()

    # ========================================================
    # CUSTOMER REFERENCE
    #
    # IMPORTANT:
    #
    # DO NOT READ OLD RAW CUSTOMER CSV FILES.
    #
    # Use curated Parquet.
    # ========================================================

    customers_file = (
        CURATED_FOLDER
        / "customers"
        / "customers.parquet"
    )

    if customers_file.exists():

        print()

        print(
            f"Reading curated customer reference: "
            f"{customers_file}"
        )

        customers_df = pd.read_parquet(
            customers_file,
            columns=["customer_id"]
        )

        customers_df["customer_id"] = pd.to_numeric(
            customers_df["customer_id"],
            errors="coerce"
        ).astype("Int64")

        customers_df = (
            customers_df[
                ["customer_id"]
            ]
            .dropna()
            .drop_duplicates()
            .rename(
                columns={
                    "customer_id":
                    "ref_customer_id"
                }
            )
        )

    else:

        print()

        print(
            "WARNING: Curated customer reference "
            "does not exist."
        )

        customers_df = pd.DataFrame(
            columns=[
                "ref_customer_id"
            ]
        )

    # ========================================================
    # PRODUCT REFERENCE
    #
    # IMPORTANT:
    #
    # DO NOT READ OLD RAW PRODUCT CSV FILES.
    #
    # Use curated Parquet.
    # ========================================================

    products_file = (
        CURATED_FOLDER
        / "products"
        / "products.parquet"
    )

    if products_file.exists():

        print()

        print(
            f"Reading curated product reference: "
            f"{products_file}"
        )

        products_df = pd.read_parquet(
            products_file,
            columns=["product_id"]
        )

        products_df["product_id"] = pd.to_numeric(
            products_df["product_id"],
            errors="coerce"
        ).astype("Int64")

        products_df = (
            products_df[
                ["product_id"]
            ]
            .dropna()
            .drop_duplicates()
            .rename(
                columns={
                    "product_id":
                    "ref_product_id"
                }
            )
        )

    else:

        print()

        print(
            "WARNING: Curated product reference "
            "does not exist."
        )

        products_df = pd.DataFrame(
            columns=[
                "ref_product_id"
            ]
        )

    # ========================================================
    # FOREIGN KEY VALIDATION
    # ========================================================

    valid_df = valid_df.merge(
        customers_df,
        left_on="customer_id",
        right_on="ref_customer_id",
        how="left"
    )

    valid_df = valid_df.merge(
        products_df,
        left_on="product_id",
        right_on="ref_product_id",
        how="left"
    )

    missing_customer = (
        valid_df[
            "ref_customer_id"
        ].isna()
    )

    missing_product = (
        valid_df[
            "ref_product_id"
        ].isna()
    )

    foreign_key_invalid = (
        missing_customer
        |
        missing_product
    )

    fk_invalid_df = valid_df[
        foreign_key_invalid
    ].copy()

    valid_df = valid_df[
        ~foreign_key_invalid
    ].copy()

    # ========================================================
    # REMOVE REFERENCE COLUMNS
    # ========================================================

    valid_df = valid_df.drop(
        columns=[
            "ref_customer_id",
            "ref_product_id"
        ],
        errors="ignore"
    )

    fk_invalid_df = fk_invalid_df.drop(
        columns=[
            "ref_customer_id",
            "ref_product_id"
        ],
        errors="ignore"
    )

    # ========================================================
    # COMBINE INVALID RECORDS
    # ========================================================

    if not fk_invalid_df.empty:

        invalid_df = pd.concat(
            [
                invalid_df,
                fk_invalid_df
            ],
            ignore_index=True
        )

    # ========================================================
    # CALCULATE TOTAL AMOUNT
    # ========================================================

    valid_df["total_amount"] = (
        valid_df["quantity"]
        *
        valid_df["unit_price"]
    )

    # ========================================================
    # DUPLICATE ORDERS
    # ========================================================

    valid_df = (
        valid_df
        .drop_duplicates(
            subset=["order_id"],
            keep="last"
        )
    )

    # ========================================================
    # QUARANTINE
    # ========================================================

    write_quarantine(
        invalid_df,
        "orders"
    )

    # ========================================================
    # PROCESSED
    # ========================================================

    write_processed(
        valid_df,
        "orders",
        append=False
    )

    # ========================================================
    # CURATED
    # ========================================================

    curated_df = valid_df.copy()

    curated_df["processed_timestamp"] = (
        pd.Timestamp.now()
    )

    # ========================================================
    # FAST UPSERT
    # ========================================================

    incremental_upsert(
        curated_df,
        "orders",
        "order_id"
    )

    return (
        len(valid_df),
        latest_time,
        selected_files
    )


# ============================================================
# ORDERS ENRICHED ETL
# ============================================================

def process_orders_enriched(
    dataset_path,
    control
):

    print()

    print("=" * 80)
    print("INCREMENTAL ORDERS ENRICHED ETL")
    print("=" * 80)

    (
        df,
        latest_time,
        selected_files
    ) = read_incremental_dataset(
        dataset_path,
        "orders_enriched",
        control
    )

    if df.empty:

        return (
            0,
            latest_time,
            selected_files
        )

    # ========================================================
    # CLEAN COLUMNS
    # ========================================================

    df = clean_column_names(
        df
    )

    # ========================================================
    # BASIC VALIDATION
    # ========================================================

    if "order_id" not in df.columns:

        raise Exception(
            "Missing required column: order_id"
        )

    # ========================================================
    # TYPE CONVERSION
    # ========================================================

    df["order_id"] = pd.to_numeric(
        df["order_id"],
        errors="coerce"
    ).astype("Int64")

    if "customer_id" in df.columns:

        df["customer_id"] = pd.to_numeric(
            df["customer_id"],
            errors="coerce"
        ).astype("Int64")

    if "product_id" in df.columns:

        df["product_id"] = pd.to_numeric(
            df["product_id"],
            errors="coerce"
        ).astype("Int64")

    if "quantity" in df.columns:

        df["quantity"] = pd.to_numeric(
            df["quantity"],
            errors="coerce"
        )

    if "unit_price" in df.columns:

        df["unit_price"] = pd.to_numeric(
            df["unit_price"],
            errors="coerce"
        )

    if "order_date" in df.columns:

        df["order_date"] = pd.to_datetime(
            df["order_date"],
            errors="coerce"
        )

    # ========================================================
    # VALIDATION
    # ========================================================

    invalid_mask = (
        df["order_id"].isna()
    )

    if "quantity" in df.columns:

        invalid_mask = (
            invalid_mask
            |
            df["quantity"].isna()
            |
            (df["quantity"] <= 0)
        )

    if "unit_price" in df.columns:

        invalid_mask = (
            invalid_mask
            |
            df["unit_price"].isna()
            |
            (df["unit_price"] < 0)
        )

    invalid_df = df[
        invalid_mask
    ].copy()

    valid_df = df[
        ~invalid_mask
    ].copy()

    # ========================================================
    # DERIVED TOTAL
    # ========================================================

    if (
        "quantity" in valid_df.columns
        and
        "unit_price" in valid_df.columns
    ):

        valid_df["total_amount"] = (
            valid_df["quantity"]
            *
            valid_df["unit_price"]
        )

    # ========================================================
    # DUPLICATES
    # ========================================================

    valid_df = (
        valid_df
        .drop_duplicates(
            subset=["order_id"],
            keep="last"
        )
    )

    # ========================================================
    # QUARANTINE
    # ========================================================

    write_quarantine(
        invalid_df,
        "orders_enriched"
    )

    # ========================================================
    # PROCESSED
    # ========================================================

    write_processed(
        valid_df,
        "orders_enriched",
        append=False
    )

    # ========================================================
    # CURATED
    # ========================================================

    curated_df = valid_df.copy()

    curated_df["processed_timestamp"] = (
        pd.Timestamp.now()
    )

    # ========================================================
    # UPSERT
    # ========================================================

    incremental_upsert(
        curated_df,
        "orders_enriched",
        "order_id"
    )

    return (
        len(valid_df),
        latest_time,
        selected_files
    )


# ============================================================
# ORDER SUMMARY
# ============================================================

def process_order_summary():

    print()

    print("=" * 80)
    print("ORDER SUMMARY")
    print("=" * 80)

    orders_file = (
        CURATED_FOLDER
        / "orders"
        / "orders.parquet"
    )

    if not orders_file.exists():

        print(
            "Orders curated file does not exist."
        )

        return

    print(
        f"Reading curated orders: {orders_file}"
    )

    orders_df = pd.read_parquet(
        orders_file
    )

    if orders_df.empty:

        print(
            "No orders available for summary."
        )

        return

    # ========================================================
    # SUMMARY
    # ========================================================

    summary_df = (
        orders_df
        .groupby(
            [
                "order_date"
            ],
            dropna=False
        )
        .agg(
            total_orders=(
                "order_id",
                "nunique"
            ),

            total_quantity=(
                "quantity",
                "sum"
            ),

            total_sales=(
                "total_amount",
                "sum"
            )
        )
        .reset_index()
    )

    summary_df["processed_timestamp"] = (
        pd.Timestamp.now()
    )

    # ========================================================
    # WRITE SUMMARY
    # ========================================================

    summary_path = (
        CURATED_FOLDER
        / "order_summary"
    )

    summary_path.mkdir(
        parents=True,
        exist_ok=True
    )

    summary_file = (
        summary_path
        / "order_summary.parquet"
    )

    summary_df = prepare_for_parquet(
        summary_df
    )

    summary_df.to_parquet(
        summary_file,
        index=False
    )

    print()

    print(
        f"Order summary written: {summary_file}"
    )

    print(
        f"Summary records: {len(summary_df)}"
    )


# ============================================================
# MAIN ETL
# ============================================================

def main():

    control = load_control()

    # ========================================================
    # DATASET PATHS
    # ========================================================

    customers_path = (
        LOCAL_FOLDER / "customers"
    )

    products_path = (
        LOCAL_FOLDER / "products"
    )

    orders_path = (
        LOCAL_FOLDER / "orders"
    )

    orders_enriched_path = (
        LOCAL_FOLDER / "orders_enriched"
    )

    # ========================================================
    # CUSTOMER
    # ========================================================

    try:

        (
            customer_count,
            customer_time,
            customer_files
        ) = process_customers(
            customers_path,
            control
        )

        if customer_files:

            mark_files_processed(
                control,
                "customers",
                customers_path,
                customer_files,
                customer_count
            )

            save_control(
                control
            )

    except Exception as e:

        print()

        print(
            f"CUSTOMER ETL FAILED: {e}"
        )

        raise

    # ========================================================
    # PRODUCT
    # ========================================================

    try:

        (
            product_count,
            product_time,
            product_files
        ) = process_products(
            products_path,
            control
        )

        if product_files:

            mark_files_processed(
                control,
                "products",
                products_path,
                product_files,
                product_count
            )

            save_control(
                control
            )

    except Exception as e:

        print()

        print(
            f"PRODUCT ETL FAILED: {e}"
        )

        raise

    # ========================================================
    # ORDERS
    # ========================================================

    try:

        (
            order_count,
            order_time,
            order_files
        ) = process_orders(
            orders_path,
            control
        )

        if order_files:

            mark_files_processed(
                control,
                "orders",
                orders_path,
                order_files,
                order_count
            )

            save_control(
                control
            )

    except Exception as e:

        print()

        print(
            f"ORDER ETL FAILED: {e}"
        )

        raise

    # ========================================================
    # ORDERS ENRICHED
    # ========================================================

    try:

        (
            enriched_count,
            enriched_time,
            enriched_files
        ) = process_orders_enriched(
            orders_enriched_path,
            control
        )

        if enriched_files:

            mark_files_processed(
                control,
                "orders_enriched",
                orders_enriched_path,
                enriched_files,
                enriched_count
            )

            save_control(
                control
            )

    except Exception as e:

        print()

        print(
            f"ORDERS ENRICHED ETL FAILED: {e}"
        )

        raise

    # ========================================================
    # ORDER SUMMARY
    # ========================================================

    try:

        process_order_summary()

    except Exception as e:

        print()

        print(
            f"ORDER SUMMARY FAILED: {e}"
        )

        raise

    # ========================================================
    # FINAL STATUS
    # ========================================================

    print()

    print("=" * 80)
    print("INCREMENTAL ETL JOB COMPLETED")
    print("=" * 80)

    print()

    print("IMPORTANT:")

    print(
        "Previously processed RAW CSV files were "
        "not reread."
    )

    print(
        "Only NEW / MODIFIED RAW CSV files were "
        "read."
    )

    print(
        "Existing curated Parquet files were used "
        "for UPSERT and reference validation."
    )

    print()

    print(
        f"Control file: {CONTROL_FILE}"
    )

    print("=" * 80)


# ============================================================
# PROGRAM ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()