import pandas as pd
from pathlib import Path

# Existing dataset folder
SOURCE_DIR = Path.cwd() / "customer_order_dataset"


# New structured data folder
OUTPUT_DIR = Path.cwd() /"raw" 


# ============================================================
# CREATE OUTPUT DIRECTORY
# ============================================================

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# CHECK SOURCE DIRECTORY
# ============================================================

if not SOURCE_DIR.exists():

    print("=" * 70)
    print("ERROR")
    print("=" * 70)

    print(f"Source folder does not exist:")
    print(SOURCE_DIR)

    print()
    print("Please generate the customer/order dataset first.")

    raise SystemExit(1)


print("=" * 70)
print("DATA PARTITIONING STARTED")
print("=" * 70)

print()
print(f"Source : {SOURCE_DIR}")
print(f"Output : {OUTPUT_DIR}")
print()


# ============================================================
# FIND ALL CSV FILES
# ============================================================

csv_files = list(
    SOURCE_DIR.glob("*.csv")
)


if not csv_files:

    print("No CSV files found in source folder.")
    raise SystemExit(1)


print(
    f"Found {len(csv_files)} CSV file(s):"
)

for file in csv_files:
    print(f"  - {file.name}")

print()


# ============================================================
# PROCESS EACH CSV FILE
# ============================================================

for source_file in csv_files:

    print("=" * 70)
    print(f"Processing: {source_file.name}")
    print("=" * 70)

    # --------------------------------------------------------
    # Read CSV
    # --------------------------------------------------------

    df = pd.read_csv(
        source_file
    )

    print(
        f"Records found: {len(df):,}"
    )

    print(
        f"Columns found: {list(df.columns)}"
    )

    # --------------------------------------------------------
    # File name without extension
    # --------------------------------------------------------

    dataset_name = source_file.stem

    # --------------------------------------------------------
    # Detect date column
    # --------------------------------------------------------

    date_columns = [
        "order_date",
        "signup_date",
        "date",
        "event_date",
        "transaction_date"
    ]

    detected_date_column = None

    for column in date_columns:

        if column in df.columns:

            detected_date_column = column
            break

    # ========================================================
    # CASE 1: DATASET HAS A DATE COLUMN
    # ========================================================

    if detected_date_column is not None:

        print(
            f"Date column detected: "
            f"{detected_date_column}"
        )

        # Convert date column
        df[detected_date_column] = pd.to_datetime(
            df[detected_date_column],
            errors="coerce"
        )

        # Check invalid dates
        invalid_dates = (
            df[detected_date_column]
            .isna()
            .sum()
        )

        if invalid_dates > 0:

            print(
                f"WARNING: {invalid_dates:,} "
                f"records have invalid dates."
            )

        # Remove rows with invalid dates
        valid_df = df[
            df[detected_date_column].notna()
        ].copy()

        # ----------------------------------------------------
        # Create daily partitions
        # ----------------------------------------------------

        for date_value, daily_df in valid_df.groupby(
            valid_df[detected_date_column].dt.date
        ):

            date_string = date_value.strftime(
                "%Y-%m-%d"
            )

            # Example:
            # data/orders/2026-08-31/
            # data/orders_enriched/2026-08-31/

            daily_output_dir = (
                OUTPUT_DIR
                / dataset_name
                / date_string
            )

            daily_output_dir.mkdir(
                parents=True,
                exist_ok=True
            )

            output_file = (
                daily_output_dir
                / f"{dataset_name}.csv"
            )

            # Save daily file
            daily_df.to_csv(
                output_file,
                index=False
            )

            print(
                f"  {date_string} -> "
                f"{len(daily_df):,} records"
            )

    # ========================================================
    # CASE 2: DATASET DOES NOT HAVE A DATE COLUMN
    # ========================================================

    else:

        print(
            "No date column detected."
        )

        # Create normal dataset directory
        dataset_output_dir = (
            OUTPUT_DIR
            / dataset_name
        )

        dataset_output_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        output_file = (
            dataset_output_dir
            / source_file.name
        )

        # Save complete dataset
        df.to_csv(
            output_file,
            index=False
        )

        print(
            f"Saved complete dataset -> "
            f"{output_file}"
        )

    print()


# ============================================================
# FINAL DIRECTORY SUMMARY
# ============================================================

print("=" * 70)
print("DATA PARTITIONING COMPLETED")
print("=" * 70)

print()
print("Output structure:")

for path in sorted(
    OUTPUT_DIR.rglob("*")
):

    if path.is_file():

        relative_path = path.relative_to(
            OUTPUT_DIR
        )

        print(
            f"  {relative_path}"
        )


# ============================================================
# FINAL MESSAGE
# ============================================================

print()
print("=" * 70)
print("SUCCESS")
print("=" * 70)

print()
print(f"Structured data created at:")
print(OUTPUT_DIR)

print()
print("Datasets processed:")

for source_file in csv_files:

    print(
        f"  ✓ {source_file.name}"
    )

print()
print("You can now use this structure as")
print("the local simulation of your S3 data lake.")

