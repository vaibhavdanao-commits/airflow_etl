import sys
from pathlib import Path

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
# DISPLAY CONFIGURATION
# ============================================================

print("=" * 80)
print("RETAIL GENERIC ETL JOB STARTED - LOCAL PANDAS")
print("=" * 80)

print(f"Raw Folder        : {LOCAL_FOLDER}")
print(f"Processed Folder  : {PROCESSED_FOLDER}")
print(f"Curated Folder    : {CURATED_FOLDER}")
print(f"Quarantine Folder : {QUARANTINE_FOLDER}")

print("=" * 80)


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def clean_column_names(df):
    """
    Standardize column names:
    - Remove leading/trailing spaces
    - Convert to lowercase
    - Replace spaces with _
    - Replace - with _
    """

    df = df.copy()

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )

    return df


# ============================================================
# FIND CSV FILES
# ============================================================

def find_csv_files(dataset_path):
    """
    Recursively find all CSV files inside dataset folder.

    Example:

    raw/
        orders/
            2026-09-10/
                orders.csv
            2026-09-11/
                orders.csv
    """

    csv_files = list(Path(dataset_path).rglob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found under: {dataset_path}"
        )

    return csv_files


# ============================================================
# WRITE PROCESSED DATA
# ============================================================

def write_processed(df, dataset):

    output_path = PROCESSED_FOLDER / dataset

    output_path.mkdir(
        parents=True,
        exist_ok=True
    )

    print(
        f"Writing processed data to: {output_path}"
    )

    output_file = (
        output_path /
        f"{dataset}.parquet"
    )

    df.to_parquet(
        output_file,
        index=False
    )

    print(
        f"Processed file created: {output_file}"
    )


# ============================================================
# WRITE CURATED DATA
# ============================================================

def write_curated(df, dataset):

    output_path = CURATED_FOLDER / dataset

    output_path.mkdir(
        parents=True,
        exist_ok=True
    )

    print(
        f"Writing curated data to: {output_path}"
    )

    output_file = (
        output_path /
        f"{dataset}.parquet"
    )

    df.to_parquet(
        output_file,
        index=False
    )

    print(
        f"Curated file created: {output_file}"
    )


# ============================================================
# WRITE QUARANTINE DATA
# ============================================================

def write_quarantine(df, dataset):

    if df.empty:

        print(
            f"No invalid records for {dataset}"
        )

        return

    output_path = QUARANTINE_FOLDER / dataset

    output_path.mkdir(
        parents=True,
        exist_ok=True
    )

    print(
        f"Writing invalid records to: {output_path}"
    )

    output_file = (
        output_path /
        f"{dataset}_invalid.parquet"
    )

    df.to_parquet(
        output_file,
        index=False
    )

    print(
        f"Quarantine file created: {output_file}"
    )


# ============================================================
# REQUIRED COLUMN VALIDATION
# ============================================================

def require_columns(df, required_columns):

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
# READ DATASET
# ============================================================

def read_dataset(dataset_path):

    print()
    print(
        f"Reading data from: {dataset_path}"
    )

    csv_files = find_csv_files(
        dataset_path
    )

    print(
        f"CSV files found: {len(csv_files)}"
    )

    dataframes = []

    for csv_file in csv_files:

        print(
            f"Reading: {csv_file}"
        )

        temp_df = pd.read_csv(
            csv_file,
            dtype=str
        )

        dataframes.append(
            temp_df
        )

    df = pd.concat(
        dataframes,
        ignore_index=True
    )

    print(
        f"Initial Record Count: {len(df)}"
    )

    print("Raw Columns:")
    print(
        list(df.columns)
    )

    df = clean_column_names(
        df
    )

    print("Standardized Columns:")
    print(
        list(df.columns)
    )

    return df


# ============================================================
# CUSTOMER ETL
# ============================================================

def process_customers(dataset_path):

    print()
    print("=" * 80)
    print("CUSTOMER ETL")
    print("=" * 80)

    df = read_dataset(
        dataset_path
    )

    required_columns = [
        "customer_id",
        "name",
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

    # --------------------------------------------------------
    # DATA TYPE CONVERSION
    # --------------------------------------------------------

    df["customer_id"] = pd.to_numeric(
        df["customer_id"],
        errors="coerce"
    ).astype("Int64")

    df["name"] = (
        df["name"]
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

    # --------------------------------------------------------
    # EMAIL VALIDATION
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # REMOVE DUPLICATES
    # --------------------------------------------------------

    duplicate_free_df = (
        valid_df
        .drop_duplicates(
            subset=["customer_id"]
        )
    )

    # --------------------------------------------------------
    # QUARANTINE
    # --------------------------------------------------------

    write_quarantine(
        invalid_df,
        "customers"
    )

    # --------------------------------------------------------
    # PROCESSED
    # --------------------------------------------------------

    write_processed(
        duplicate_free_df,
        "customers"
    )

    # --------------------------------------------------------
    # CURATED
    # --------------------------------------------------------

    curated_df = duplicate_free_df.copy()

    curated_df["customer_name"] = (
        curated_df["name"]
        .astype("string")
        .str.title()
    )

    curated_df["processed_timestamp"] = pd.Timestamp.now()

    write_curated(
        curated_df,
        "customers"
    )


# ============================================================
# PRODUCT ETL
# ============================================================

def process_products(dataset_path):

    print()
    print("=" * 80)
    print("PRODUCT ETL")
    print("=" * 80)

    df = read_dataset(
        dataset_path
    )

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

    # --------------------------------------------------------
    # DATA TYPE CONVERSION
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # DATA QUALITY
    # --------------------------------------------------------

    invalid_mask = (
        df["product_id"].isna()
        |
        df["product_name"].isna()
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

    # --------------------------------------------------------
    # REMOVE DUPLICATES
    # --------------------------------------------------------

    valid_df = (
        valid_df
        .drop_duplicates(
            subset=["product_id"]
        )
    )

    # --------------------------------------------------------
    # QUARANTINE
    # --------------------------------------------------------

    write_quarantine(
        invalid_df,
        "products"
    )

    # --------------------------------------------------------
    # PROCESSED
    # --------------------------------------------------------

    write_processed(
        valid_df,
        "products"
    )

    # --------------------------------------------------------
    # CURATED
    # --------------------------------------------------------

    curated_df = valid_df.copy()

    curated_df["product_name"] = (
        curated_df["product_name"]
        .astype("string")
        .str.title()
    )

    curated_df["processed_timestamp"] = pd.Timestamp.now()

    write_curated(
        curated_df,
        "products"
    )


# ============================================================
# ORDERS ETL
# ============================================================

def process_orders(dataset_path):

    print()
    print("=" * 80)
    print("ORDERS ETL")
    print("=" * 80)

    df = read_dataset(
        dataset_path
    )

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

    # --------------------------------------------------------
    # DATA TYPE CONVERSION
    # --------------------------------------------------------

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
        format="%Y-%m-%d",
        errors="coerce"
    )

    df["quantity"] = pd.to_numeric(
        df["quantity"],
        errors="coerce"
    ).astype("Int64")

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

    # --------------------------------------------------------
    # ORDER DATA QUALITY
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # REMOVE DUPLICATES
    # --------------------------------------------------------

    valid_df = (
        valid_df
        .drop_duplicates(
            subset=["order_id"]
        )
    )

    # ========================================================
    # CUSTOMER REFERENCE DATA
    # ========================================================

    customers_path = (
        LOCAL_FOLDER / "customers"
    )

    print(
        f"Reading customers from: {customers_path}"
    )

    customers_df = read_dataset(
        customers_path
    )

    customers_df["customer_id"] = pd.to_numeric(
        customers_df["customer_id"],
        errors="coerce"
    ).astype("Int64")

    customers_df = (
        customers_df[
            ["customer_id"]
        ]
        .drop_duplicates()
        .rename(
            columns={
                "customer_id":
                "ref_customer_id"
            }
        )
    )

    # ========================================================
    # PRODUCT REFERENCE DATA
    # ========================================================

    products_path = (
        LOCAL_FOLDER / "products"
    )

    print(
        f"Reading products from: {products_path}"
    )

    products_df = read_dataset(
        products_path
    )

    products_df["product_id"] = pd.to_numeric(
        products_df["product_id"],
        errors="coerce"
    ).astype("Int64")

    products_df = (
        products_df[
            ["product_id"]
        ]
        .drop_duplicates()
        .rename(
            columns={
                "product_id":
                "ref_product_id"
            }
        )
    )

    # ========================================================
    # CUSTOMER VALIDATION
    # ========================================================

    customer_join = valid_df.merge(
        customers_df,
        left_on="customer_id",
        right_on="ref_customer_id",
        how="left"
    )

    # ========================================================
    # PRODUCT VALIDATION
    # ========================================================

    customer_product_join = customer_join.merge(
        products_df,
        left_on="product_id",
        right_on="ref_product_id",
        how="left"
    )

    # ========================================================
    # INVALID FOREIGN KEYS
    # ========================================================

    fk_invalid_mask = (
        customer_product_join[
            "ref_customer_id"
        ].isna()
        |
        customer_product_join[
            "ref_product_id"
        ].isna()
    )

    fk_invalid_df = (
        customer_product_join[
            fk_invalid_mask
        ]
        .copy()
    )

    # ========================================================
    # VALID ORDERS
    # ========================================================

    valid_orders_df = (
        customer_product_join[
            ~fk_invalid_mask
        ]
        .copy()
    )

    valid_orders_df = (
        valid_orders_df
        .drop(
            columns=[
                "ref_customer_id",
                "ref_product_id"
            ]
        )
    )

    # ========================================================
    # COMBINE INVALID ORDERS
    # ========================================================

    all_invalid_orders = invalid_df.copy()

    all_invalid_orders[
        "dq_reason"
    ] = (
        "Schema or business validation failed"
    )

    fk_invalid_df = (
        fk_invalid_df
        .drop(
            columns=[
                "ref_customer_id",
                "ref_product_id"
            ]
        )
    )

    fk_invalid_df[
        "dq_reason"
    ] = (
        "Customer ID or Product ID does not exist"
    )

    # Make sure both datasets have the same columns
    quarantine_orders = pd.concat(
        [
            all_invalid_orders,
            fk_invalid_df
        ],
        ignore_index=True,
        sort=False
    )

    # ========================================================
    # QUARANTINE
    # ========================================================

    write_quarantine(
        quarantine_orders,
        "orders"
    )

    # ========================================================
    # DERIVED COLUMNS
    # ========================================================

    curated_orders = valid_orders_df.copy()

    curated_orders["order_amount"] = (
        curated_orders["quantity"].astype(float)
        *
        curated_orders["unit_price"]
    ).round(2)

    curated_orders["order_year"] = (
        curated_orders["order_date"]
        .dt.year
    )

    curated_orders["order_month"] = (
        curated_orders["order_date"]
        .dt.month
    )

    curated_orders["processed_timestamp"] = (
        pd.Timestamp.now()
    )

    # ========================================================
    # PROCESSED
    # ========================================================

    write_processed(
        curated_orders,
        "orders"
    )

    # ========================================================
    # CURATED
    # ========================================================

    write_curated(
        curated_orders,
        "orders"
    )

    # ========================================================
    # ORDER AGGREGATION
    # ========================================================

    print()
    print("Creating order aggregation...")

    order_summary = (
        curated_orders
        .groupby(
            [
                "order_date",
                "order_status"
            ],
            as_index=False
        )
        .agg(
            total_orders=(
                "order_id",
                "count"
            ),
            total_quantity=(
                "quantity",
                "sum"
            ),
            total_order_amount=(
                "order_amount",
                "sum"
            )
        )
    )

    order_summary[
        "total_order_amount"
    ] = (
        order_summary[
            "total_order_amount"
        ]
        .round(2)
    )

    aggregation_path = (
        CURATED_FOLDER / "order_summary"
    )

    aggregation_path.mkdir(
        parents=True,
        exist_ok=True
    )

    aggregation_file = (
        aggregation_path /
        "order_summary.parquet"
    )

    order_summary.to_parquet(
        aggregation_file,
        index=False
    )

    print(
        f"Order summary written to: {aggregation_file}"
    )


# ============================================================
# ORDERS ENRICHED ETL
# ============================================================

def process_orders_enriched(dataset_path):

    print()
    print("=" * 80)
    print("ORDERS ENRICHED ETL")
    print("=" * 80)

    df = read_dataset(
        dataset_path
    )

    df["processed_timestamp"] = (
        pd.Timestamp.now()
    )

    df = (
        df
        .drop_duplicates()
        .reset_index(drop=True)
    )

    write_processed(
        df,
        "orders_enriched"
    )

    write_curated(
        df,
        "orders_enriched"
    )


# ============================================================
# DISCOVER DATASETS
# ============================================================

print()
print("=" * 80)
print("DISCOVERING DATASETS")
print("=" * 80)

if not LOCAL_FOLDER.exists():

    raise FileNotFoundError(
        f"Raw folder does not exist: {LOCAL_FOLDER}"
    )


dataset_folders = [
    folder
    for folder in LOCAL_FOLDER.iterdir()
    if folder.is_dir()
]


if not dataset_folders:

    raise Exception(
        f"No dataset folders found under: {LOCAL_FOLDER}"
    )


print("Datasets found:")

for folder in dataset_folders:

    print(
        f" - {folder.name}"
    )


# ============================================================
# PROCESS DATASETS
#
# Order is important:
#
# customers
#      ↓
# products
#      ↓
# orders
#      ↓
# orders_enriched
#
# ============================================================

PROCESS_ORDER = [
    "customers",
    "products",
    "orders",
    "orders_enriched"
]


for dataset_type in PROCESS_ORDER:

    matching_folders = [
        folder
        for folder in dataset_folders
        if folder.name.lower() == dataset_type
    ]

    if not matching_folders:

        print()
        print(
            f"No {dataset_type} dataset found. Skipping."
        )

        continue


    for dataset_folder in matching_folders:

        print()
        print("=" * 80)

        print(
            f"STARTING DATASET: {dataset_type}"
        )

        print("=" * 80)

        try:

            if dataset_type == "customers":

                process_customers(
                    dataset_folder
                )

            elif dataset_type == "products":

                process_products(
                    dataset_folder
                )

            elif dataset_type == "orders":

                process_orders(
                    dataset_folder
                )

            elif dataset_type == "orders_enriched":

                process_orders_enriched(
                    dataset_folder
                )

            print()

            print(
                f"{dataset_type} processing completed successfully."
            )

        except Exception as e:

            print()

            print(
                f"ERROR processing {dataset_type}: {e}"
            )

            raise


# ============================================================
# JOB COMPLETION
# ============================================================

print()
print("=" * 80)
print("RETAIL GENERIC ETL JOB COMPLETED - LOCAL PANDAS")
print("=" * 80)

print()

print("Output locations:")

print(
    f"Processed  : {PROCESSED_FOLDER}"
)

print(
    f"Curated    : {CURATED_FOLDER}"
)

print(
    f"Quarantine : {QUARANTINE_FOLDER}"
)

print("=" * 80)
