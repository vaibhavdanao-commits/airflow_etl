import sys
from pathlib import Path

# Get src folder
SRC_FOLDER = Path(__file__).resolve().parent.parent

# Add src folder to Python path
sys.path.insert(0, str(SRC_FOLDER))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    DoubleType
)


# ============================================================
# PROJECT ROOT / CONFIG IMPORT
# ============================================================



from config import (
    LOCAL_FOLDER,
    PROCESSED_FOLDER,
    CURATED_FOLDER,
    QUARANTINE_FOLDER
)


# ============================================================
# SPARK INITIALIZATION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Retail_Generic_ETL_Local")
    .master("local[*]")
    .config(
        "spark.sql.legacy.timeParserPolicy",
        "LEGACY"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# DISPLAY CONFIGURATION
# ============================================================

print("=" * 80)
print("RETAIL GENERIC ETL JOB STARTED - LOCAL")
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

    for column in df.columns:

        new_name = (
            column.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )

        df = df.withColumnRenamed(
            column,
            new_name
        )

    return df


# ============================================================
# WRITE PROCESSED DATA
# ============================================================

def write_processed(df, dataset):

    output_path = PROCESSED_FOLDER / dataset

    print(
        f"Writing processed data to: {output_path}"
    )

    (
        df.write
        .mode("append")
        .parquet(str(output_path))
    )


# ============================================================
# WRITE CURATED DATA
# ============================================================

def write_curated(df, dataset):

    output_path = CURATED_FOLDER / dataset

    print(
        f"Writing curated data to: {output_path}"
    )

    (
        df.write
        .mode("append")
        .parquet(str(output_path))
    )


# ============================================================
# WRITE QUARANTINE DATA
# ============================================================

def write_quarantine(df, dataset):

    if df.rdd.isEmpty():

        print(
            f"No invalid records for {dataset}"
        )

        return

    output_path = QUARANTINE_FOLDER / dataset

    print(
        f"Writing invalid records to: {output_path}"
    )

    (
        df.write
        .mode("append")
        .parquet(str(output_path))
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

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(str(dataset_path))
    )

    print(
        f"Initial Record Count: {df.count()}"
    )

    print("Raw Schema:")

    df.printSchema()

    df = clean_column_names(df)

    print("Standardized Columns:")

    print(df.columns)

    return df


# ============================================================
# CUSTOMER ETL
# ============================================================

def process_customers(dataset_path):

    print()
    print("=" * 80)
    print("CUSTOMER ETL")
    print("=" * 80)

    df = read_dataset(dataset_path)

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

    df = (
        df
        .withColumn(
            "customer_id",
            F.col("customer_id").cast(IntegerType())
        )
        .withColumn(
            "name",
            F.trim(F.col("name"))
        )
        .withColumn(
            "email",
            F.lower(
                F.trim(F.col("email"))
            )
        )
        .withColumn(
            "city",
            F.trim(F.col("city"))
        )
        .withColumn(
            "state",
            F.upper(
                F.trim(F.col("state"))
            )
        )
        .withColumn(
            "signup_date",
            F.to_date(
                F.col("signup_date"),
                "yyyy-MM-dd"
            )
        )
        .withColumn(
            "customer_segment",
            F.upper(
                F.trim(F.col("customer_segment"))
            )
        )
    )

    # --------------------------------------------------------
    # EMAIL VALIDATION
    # --------------------------------------------------------

    email_pattern = (
        r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    )

    invalid_df = df.filter(
        F.col("customer_id").isNull()
        |
        F.col("email").isNull()
        |
        ~F.col("email").rlike(email_pattern)
    )

    valid_df = df.filter(
        F.col("customer_id").isNotNull()
        &
        F.col("email").isNotNull()
        &
        F.col("email").rlike(email_pattern)
    )

    # --------------------------------------------------------
    # REMOVE DUPLICATES
    # --------------------------------------------------------

    duplicate_free_df = valid_df.dropDuplicates(
        ["customer_id"]
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

    curated_df = (
        duplicate_free_df
        .withColumn(
            "customer_name",
            F.initcap(
                F.col("name")
            )
        )
        .withColumn(
            "processed_timestamp",
            F.current_timestamp()
        )
    )

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

    df = read_dataset(dataset_path)

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

    df = (
        df
        .withColumn(
            "product_id",
            F.col("product_id").cast(IntegerType())
        )
        .withColumn(
            "product_name",
            F.trim(
                F.col("product_name")
            )
        )
        .withColumn(
            "category",
            F.upper(
                F.trim(F.col("category"))
            )
        )
        .withColumn(
            "price",
            F.col("price").cast(DoubleType())
        )
        .withColumn(
            "supplier",
            F.trim(
                F.col("supplier")
            )
        )
    )

    # --------------------------------------------------------
    # DATA QUALITY
    # --------------------------------------------------------

    invalid_df = df.filter(
        F.col("product_id").isNull()
        |
        F.col("product_name").isNull()
        |
        F.col("price").isNull()
        |
        (F.col("price") < 0)
    )

    valid_df = df.filter(
        F.col("product_id").isNotNull()
        &
        F.col("product_name").isNotNull()
        &
        F.col("price").isNotNull()
        &
        (F.col("price") >= 0)
    )

    # --------------------------------------------------------
    # REMOVE DUPLICATES
    # --------------------------------------------------------

    valid_df = valid_df.dropDuplicates(
        ["product_id"]
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

    curated_df = (
        valid_df
        .withColumn(
            "product_name",
            F.initcap(
                F.col("product_name")
            )
        )
        .withColumn(
            "processed_timestamp",
            F.current_timestamp()
        )
    )

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

    df = read_dataset(dataset_path)

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

    df = (
        df
        .withColumn(
            "order_id",
            F.col("order_id").cast(IntegerType())
        )
        .withColumn(
            "customer_id",
            F.col("customer_id").cast(IntegerType())
        )
        .withColumn(
            "product_id",
            F.col("product_id").cast(IntegerType())
        )
        .withColumn(
            "order_date",
            F.to_date(
                F.col("order_date"),
                "yyyy-MM-dd"
            )
        )
        .withColumn(
            "quantity",
            F.col("quantity").cast(IntegerType())
        )
        .withColumn(
            "unit_price",
            F.col("unit_price").cast(DoubleType())
        )
        .withColumn(
            "payment_method",
            F.upper(
                F.trim(F.col("payment_method"))
            )
        )
        .withColumn(
            "order_status",
            F.upper(
                F.trim(F.col("order_status"))
            )
        )
        .withColumn(
            "store_id",
            F.trim(
                F.col("store_id")
            )
        )
    )

    # --------------------------------------------------------
    # ORDER DATA QUALITY
    # --------------------------------------------------------

    invalid_df = df.filter(
        F.col("order_id").isNull()
        |
        F.col("customer_id").isNull()
        |
        F.col("product_id").isNull()
        |
        F.col("order_date").isNull()
        |
        F.col("quantity").isNull()
        |
        (F.col("quantity") <= 0)
        |
        F.col("unit_price").isNull()
        |
        (F.col("unit_price") < 0)
    )

    valid_df = df.filter(
        F.col("order_id").isNotNull()
        &
        F.col("customer_id").isNotNull()
        &
        F.col("product_id").isNotNull()
        &
        F.col("order_date").isNotNull()
        &
        F.col("quantity").isNotNull()
        &
        (F.col("quantity") > 0)
        &
        F.col("unit_price").isNotNull()
        &
        (F.col("unit_price") >= 0)
    )

    # --------------------------------------------------------
    # REMOVE DUPLICATES
    # --------------------------------------------------------

    valid_df = valid_df.dropDuplicates(
        ["order_id"]
    )

    # ========================================================
    # CUSTOMER REFERENCE DATA
    # ========================================================

    customers_path = LOCAL_FOLDER / "customers"

    print(
        f"Reading customers from: {customers_path}"
    )

    customers_df = (
        spark.read
        .option("header", "true")
        .csv(str(customers_path))
    )

    customers_df = clean_column_names(
        customers_df
    )

    customers_df = (
        customers_df
        .select(
            F.col("customer_id")
            .cast(IntegerType())
            .alias("ref_customer_id")
        )
        .dropDuplicates()
    )

    # ========================================================
    # PRODUCT REFERENCE DATA
    # ========================================================

    products_path = LOCAL_FOLDER / "products"

    print(
        f"Reading products from: {products_path}"
    )

    products_df = (
        spark.read
        .option("header", "true")
        .csv(str(products_path))
    )

    products_df = clean_column_names(
        products_df
    )

    products_df = (
        products_df
        .select(
            F.col("product_id")
            .cast(IntegerType())
            .alias("ref_product_id")
        )
        .dropDuplicates()
    )

    # ========================================================
    # CUSTOMER VALIDATION
    # ========================================================

    customer_join = (
        valid_df
        .join(
            customers_df,
            valid_df.customer_id
            == customers_df.ref_customer_id,
            "left"
        )
    )

    # ========================================================
    # PRODUCT VALIDATION
    # ========================================================

    customer_product_join = (
        customer_join
        .join(
            products_df,
            customer_join.product_id
            == products_df.ref_product_id,
            "left"
        )
    )

    # ========================================================
    # INVALID FOREIGN KEYS
    # ========================================================

    fk_invalid_df = (
        customer_product_join
        .filter(
            F.col("ref_customer_id").isNull()
            |
            F.col("ref_product_id").isNull()
        )
    )

    # ========================================================
    # VALID ORDERS
    # ========================================================

    valid_orders_df = (
        customer_product_join
        .filter(
            F.col("ref_customer_id").isNotNull()
            &
            F.col("ref_product_id").isNotNull()
        )
        .drop(
            "ref_customer_id",
            "ref_product_id"
        )
    )

    # ========================================================
    # COMBINE INVALID ORDERS
    # ========================================================

    all_invalid_orders = (
        invalid_df
        .withColumn(
            "dq_reason",
            F.lit(
                "Schema or business validation failed"
            )
        )
    )

    fk_invalid_df = (
        fk_invalid_df
        .drop(
            "ref_customer_id",
            "ref_product_id"
        )
        .withColumn(
            "dq_reason",
            F.lit(
                "Customer ID or Product ID does not exist"
            )
        )
    )

    quarantine_orders = (
        all_invalid_orders
        .unionByName(
            fk_invalid_df,
            allowMissingColumns=True
        )
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

    curated_orders = (
        valid_orders_df
        .withColumn(
            "order_amount",
            F.round(
                F.col("quantity")
                * F.col("unit_price"),
                2
            )
        )
        .withColumn(
            "order_year",
            F.year(
                F.col("order_date")
            )
        )
        .withColumn(
            "order_month",
            F.month(
                F.col("order_date")
            )
        )
        .withColumn(
            "processed_timestamp",
            F.current_timestamp()
        )
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
        .groupBy(
            "order_date",
            "order_status"
        )
        .agg(
            F.count(
                "order_id"
            ).alias(
                "total_orders"
            ),
            F.sum(
                "quantity"
            ).alias(
                "total_quantity"
            ),
            F.round(
                F.sum("order_amount"),
                2
            ).alias(
                "total_order_amount"
            )
        )
    )

    aggregation_path = (
        CURATED_FOLDER / "order_summary"
    )

    (
        order_summary.write
        .mode("append")
        .parquet(
            str(aggregation_path)
        )
    )

    print(
        f"Order summary written to: {aggregation_path}"
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

    df = (
        df
        .withColumn(
            "processed_timestamp",
            F.current_timestamp()
        )
    )

    df = df.dropDuplicates()

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
# customers -> products -> orders -> orders_enriched
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
print("RETAIL GENERIC ETL JOB COMPLETED - LOCAL")
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


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()