from pathlib import Path
from datetime import date, timedelta

import polars as pl
from pyiceberg.catalog import load_catalog


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

WAREHOUSE_FOLDER = PROJECT_ROOT / "Warehouse"

CATALOG_DB = WAREHOUSE_FOLDER / "dimensional_model_catalog.db"

DIMENSIONAL_WAREHOUSE_FOLDER = (
    WAREHOUSE_FOLDER / "dimensional_model"
)

WAREHOUSE_URI = DIMENSIONAL_WAREHOUSE_FOLDER.resolve().as_uri()

NAMESPACE = "star_schema"




catalog = load_catalog(
    "dimensional_model",
    type="sql",
    uri=f"sqlite:///{CATALOG_DB}",
    warehouse=WAREHOUSE_URI,
    **{
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
    },
)


# ============================================================
# 3. TABLE IDENTIFIERS
# ============================================================

DIM_CUSTOMER = f"{NAMESPACE}.dim_customer"
DIM_PRODUCT = f"{NAMESPACE}.dim_product"
DIM_STORE = f"{NAMESPACE}.dim_store"
DIM_DATE = f"{NAMESPACE}.dim_date"
FACT_SALES = f"{NAMESPACE}.fact_sales"


# ============================================================
# 4. READ ICEBERG TABLE
# ============================================================

def read_iceberg_table(table_name: str) -> pl.DataFrame:
    """
    Read an Iceberg table into Polars.
    """

    print(f"\nReading: {table_name}")

    if not catalog.table_exists(table_name):
        raise RuntimeError(
            f"Iceberg table does not exist: {table_name}"
        )

    table = catalog.load_table(table_name)

    arrow_table = table.scan().to_arrow()

    df = pl.from_arrow(arrow_table)

    print(f"Rows: {df.height}")
    print(f"Columns: {df.width}")

    return df


# ============================================================
# 5. LOAD DIMENSIONAL MODEL
# ============================================================

dim_customer = read_iceberg_table(DIM_CUSTOMER)

dim_product = read_iceberg_table(DIM_PRODUCT)

dim_store = read_iceberg_table(DIM_STORE)

dim_date = read_iceberg_table(DIM_DATE)

fact_sales = read_iceberg_table(FACT_SALES)


# ============================================================
# 6. BASIC VALIDATION
# ============================================================

required_fact_columns = [
    "sale_id",
    "customer_key",
    "product_key",
    "store_key",
    "date_key",
    "quantity",
    "sales_amount",
    "discount",
]

required_product_columns = [
    "product_key",
    "product_id",
    "product_name",
    "category",
]

required_customer_columns = [
    "customer_key",
    "customer_id",
    "customer_name",
    "city",
]

required_date_columns = [
    "date_key",
    "full_date",
]

for column in required_fact_columns:
    if column not in fact_sales.columns:
        raise ValueError(
            f"Missing fact_sales column: {column}"
        )

for column in required_product_columns:
    if column not in dim_product.columns:
        raise ValueError(
            f"Missing dim_product column: {column}"
        )

for column in required_customer_columns:
    if column not in dim_customer.columns:
        raise ValueError(
            f"Missing dim_customer column: {column}"
        )

for column in required_date_columns:
    if column not in dim_date.columns:
        raise ValueError(
            f"Missing dim_date column: {column}"
        )


# ============================================================
# 7. NORMALIZE TYPES
# ============================================================

fact_sales = fact_sales.with_columns(
    pl.col("customer_key").cast(pl.Int64),
    pl.col("product_key").cast(pl.Int64),
    pl.col("store_key").cast(pl.Int64),
    pl.col("date_key").cast(pl.Int32),
    pl.col("quantity").cast(pl.Int64),
    pl.col("sales_amount").cast(pl.Float64),
    pl.col("discount").cast(pl.Float64),
)

dim_product = dim_product.with_columns(
    pl.col("product_key").cast(pl.Int64)
)

dim_customer = dim_customer.with_columns(
    pl.col("customer_key").cast(pl.Int64)
)

dim_date = dim_date.with_columns(
    pl.col("date_key").cast(pl.Int32),
    pl.col("full_date").cast(pl.Date),
)


# ============================================================
# 8. HELPER FUNCTION
# ============================================================

def print_result(
    title: str,
    df: pl.DataFrame,
    limit: int = 20
):
    """
    Print query result in a readable format.
    """

    print("\n")
    print("=" * 100)
    print(title)
    print("=" * 100)

    print(f"Result rows: {df.height}")

    if df.is_empty():
        print("No records found.")
        return

    print(df.head(limit))



print("\n")
print("=" * 100)
print("QUESTION 1")
print("TOP 10 PRODUCTS BY REVENUE")
print("=" * 100)



q1_top_products = (
    fact_sales
    .join(
        dim_product.select([
            "product_key",
            "product_id",
            "product_name",
            "category",
        ]),
        on="product_key",
        how="inner",
    )
    .group_by([
        "product_id",
        "product_name",
        "category",
    ])
    .agg(
        pl.col("sales_amount")
        .sum()
        .alias("revenue"),

        pl.col("quantity")
        .sum()
        .alias("units_sold"),
    )
    .sort(
        "revenue",
        descending=True,
    )
    .head(10)
)

print_result(
    "Q1 — Top 10 Products by Revenue",
    q1_top_products,
    10,
)


# ============================================================
# QUESTION 2
# CITIES WITH HIGHEST SALES
# ============================================================

print("\n")
print("=" * 100)
print("QUESTION 2")
print("CITIES WITH HIGHEST SALES")
print("=" * 100)



q2_city_sales = (
    fact_sales
    .join(
        dim_customer
        .select([
            "customer_key",
            "customer_id",
            "city",
        ]),
        on="customer_key",
        how="inner",
    )
    .group_by("city")
    .agg(
        pl.col("sales_amount")
        .sum()
        .alias("total_sales"),

        pl.col("sale_id")
        .n_unique()
        .alias("orders"),
    )
    .sort(
        "total_sales",
        descending=True,
    )
)

print_result(
    "Q2 — Cities Ranked by Sales",
    q2_city_sales,
    20,
)


# ============================================================
# QUESTION 3
# MONTH-OVER-MONTH REVENUE
# ============================================================

print("\n")
print("=" * 100)
print("QUESTION 3")
print("MONTH-OVER-MONTH REVENUE")
print("=" * 100)



q3_monthly_revenue = (
    fact_sales
    .join(
        dim_date.select([
            "date_key",
            "full_date",
        ]),
        on="date_key",
        how="inner",
    )
    .with_columns(
        pl.col("full_date")
        .dt.truncate("1mo")
        .alias("month")
    )
    .group_by("month")
    .agg(
        pl.col("sales_amount")
        .sum()
        .alias("revenue"),

        pl.col("sale_id")
        .n_unique()
        .alias("orders"),
    )
    .sort("month")
)


# Calculate previous month revenue
q3_monthly_revenue = q3_monthly_revenue.with_columns(
    pl.col("revenue")
    .shift(1)
    .alias("previous_month_revenue")
)


# Calculate MoM percentage
q3_monthly_revenue = q3_monthly_revenue.with_columns(
    pl.when(
        pl.col("previous_month_revenue").is_null()
        | (pl.col("previous_month_revenue") == 0)
    )
    .then(None)
    .otherwise(
        (
            (
                pl.col("revenue")
                - pl.col("previous_month_revenue")
            )
            /
            pl.col("previous_month_revenue")
        )
        * 100
    )
    .alias("mom_growth_percent")
)


print_result(
    "Q3 — Month-over-Month Revenue",
    q3_monthly_revenue,
    q3_monthly_revenue.height,
)


# ============================================================
# QUESTION 4
# CUSTOMERS WITHOUT ORDERS IN LAST 90 DAYS
# ============================================================

print("\n")
print("=" * 100)
print("QUESTION 4")
print("CUSTOMERS WHO HAVE NOT PLACED AN ORDER IN THE LAST 90 DAYS")
print("=" * 100)



latest_order_date = (
    dim_date
    .select(
        pl.col("full_date").max()
    )
    .item()
)

if latest_order_date is None:
    raise ValueError(
        "Unable to determine latest order date."
    )

cutoff_date = (
    latest_order_date
    - timedelta(days=90)
)

print(f"Latest order date : {latest_order_date}")
print(f"90-day cutoff     : {cutoff_date}")


# Get customers who have ordered
# during the last 90 days.

recent_customer_orders = (
    fact_sales
    .join(
        dim_date.select([
            "date_key",
            "full_date",
        ]),
        on="date_key",
        how="inner",
    )
    .filter(
        pl.col("full_date") >= cutoff_date
    )
    .select("customer_key")
    .unique()
)


# Customers without recent orders

q4_inactive_customers = (
    dim_customer
    .filter(
        pl.col("is_current") == True
    )
    .join(
        recent_customer_orders,
        on="customer_key",
        how="anti",
    )
    .select([
        "customer_key",
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "customer_segment",
        "effective_from",
    ])
    .sort("customer_id")
)


print_result(
    "Q4 — Customers Without Orders in Last 90 Days",
    q4_inactive_customers,
    q4_inactive_customers.height,
)


# ============================================================
# QUESTION 5
# PRODUCT CATEGORIES WITH DECLINING SALES
# ============================================================

print("\n")
print("=" * 100)
print("QUESTION 5")
print("PRODUCT CATEGORIES WITH DECLINING SALES")
print("=" * 100)



category_monthly = (
    fact_sales
    .join(
        dim_product.select([
            "product_key",
            "category",
        ]),
        on="product_key",
        how="inner",
    )
    .join(
        dim_date.select([
            "date_key",
            "full_date",
        ]),
        on="date_key",
        how="inner",
    )
    .with_columns(
        pl.col("full_date")
        .dt.truncate("1mo")
        .alias("month")
    )
    .group_by([
        "category",
        "month",
    ])
    .agg(
        pl.col("sales_amount")
        .sum()
        .alias("revenue")
    )
    .sort([
        "category",
        "month",
    ])
)


# Find latest month

latest_month = (
    category_monthly
    .select(
        pl.col("month").max()
    )
    .item()
)


# Previous month

previous_month = (
    category_monthly
    .filter(
        pl.col("month") < latest_month
    )
    .select(
        pl.col("month").max()
    )
    .item()
)


if previous_month is None:

    print(
        "\nNot enough monthly data to calculate "
        "category decline."
    )

    q5_declining_categories = pl.DataFrame()

else:

    print(
        f"Previous month: {previous_month}"
    )

    print(
        f"Latest month  : {latest_month}"
    )

    # Latest month

    latest_category = (
        category_monthly
        .filter(
            pl.col("month") == latest_month
        )
        .select([
            "category",
            pl.col("revenue")
            .alias("latest_month_revenue"),
        ])
    )


    # Previous month

    previous_category = (
        category_monthly
        .filter(
            pl.col("month") == previous_month
        )
        .select([
            "category",
            pl.col("revenue")
            .alias("previous_month_revenue"),
        ])
    )


    # Compare

    q5_declining_categories = (
        previous_category
        .join(
            latest_category,
            on="category",
            how="left",
        )
        .with_columns(
            pl.col(
                "latest_month_revenue"
            )
            .fill_null(0)
        )
        .with_columns(
            (
                pl.col("latest_month_revenue")
                -
                pl.col("previous_month_revenue")
            )
            .alias("revenue_change")
        )
        .with_columns(
            pl.when(
                pl.col("previous_month_revenue") == 0
            )
            .then(None)
            .otherwise(
                (
                    pl.col("revenue_change")
                    /
                    pl.col(
                        "previous_month_revenue"
                    )
                )
                * 100
            )
            .alias("change_percent")
        )
        .filter(
            pl.col("revenue_change") < 0
        )
        .sort(
            "revenue_change"
        )
    )


print_result(
    "Q5 — Product Categories With Declining Sales",
    q5_declining_categories,
    q5_declining_categories.height,
)


# ============================================================
# 9. SAVE RESULTS
# ============================================================

OUTPUT_FOLDER = PROJECT_ROOT / "task9_results"

OUTPUT_FOLDER.mkdir(
    parents=True,
    exist_ok=True,
)


q1_top_products.write_csv(
    OUTPUT_FOLDER / "Q1_top_10_products_by_revenue.csv"
)

q2_city_sales.write_csv(
    OUTPUT_FOLDER / "Q2_city_sales.csv"
)

q3_monthly_revenue.write_csv(
    OUTPUT_FOLDER / "Q3_month_over_month_revenue.csv"
)

q4_inactive_customers.write_csv(
    OUTPUT_FOLDER / "Q4_customers_without_order_last_90_days.csv"
)

q5_declining_categories.write_csv(
    OUTPUT_FOLDER / "Q5_declining_product_categories.csv"
)


# ============================================================
# 10. SUMMARY
# ============================================================

print("\n")
print("=" * 100)
print("TASK 9 COMPLETED SUCCESSFULLY")
print("=" * 100)

print(
    f"""
Q1 - Top 10 products by revenue
    Result rows: {q1_top_products.height}

Q2 - Cities ranked by sales
    Result rows: {q2_city_sales.height}

Q3 - Month-over-month revenue
    Result rows: {q3_monthly_revenue.height}

Q4 - Customers without orders in last 90 days
    Result rows: {q4_inactive_customers.height}

Q5 - Product categories with declining sales
    Result rows: {q5_declining_categories.height}

Results saved to:
{OUTPUT_FOLDER}
"""
)

print("=" * 100)
print("ALL ANALYTICS QUERIES COMPLETED")
print("=" * 100)