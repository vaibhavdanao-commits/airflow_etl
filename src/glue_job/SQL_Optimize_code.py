
import sys

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8")

from pathlib import Path

import polars as pl
import duckdb
from pyiceberg.catalog import load_catalog


# ============================================================
# 1. PROJECT CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

WAREHOUSE_FOLDER = PROJECT_ROOT / "Warehouse"

CATALOG_DB = WAREHOUSE_FOLDER / "dimensional_model_catalog.db"

DIMENSIONAL_WAREHOUSE_FOLDER = (
    WAREHOUSE_FOLDER / "dimensional_model"
)

WAREHOUSE_URI = DIMENSIONAL_WAREHOUSE_FOLDER.resolve().as_uri()

NAMESPACE = "star_schema"

OUTPUT_FOLDER = PROJECT_ROOT / "task13_results"
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)


# ============================================================
# 2. LOAD ICEBERG CATALOG
# ============================================================

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
DIM_DATE = f"{NAMESPACE}.dim_date"
FACT_SALES = f"{NAMESPACE}.fact_sales"


# ============================================================
# 4. READ ICEBERG TABLES
# ============================================================

def read_iceberg_table(table_name: str) -> pl.DataFrame:

    print(f"\nReading Iceberg table: {table_name}")

    if not catalog.table_exists(table_name):
        raise RuntimeError(f"Table does not exist: {table_name}")

    table = catalog.load_table(table_name)

    df = pl.from_arrow(table.scan().to_arrow())

    print(f"Rows: {df.height:,}")
    print(f"Columns: {df.width}")

    return df


dim_customer = read_iceberg_table(DIM_CUSTOMER)
dim_product = read_iceberg_table(DIM_PRODUCT)
dim_date = read_iceberg_table(DIM_DATE)
fact_sales = read_iceberg_table(FACT_SALES)


# ============================================================
# 5. VALIDATE REQUIRED COLUMNS
# ============================================================

required_columns = {
    "fact_sales": (
        fact_sales,
        [
            "sale_id",
            "customer_key",
            "product_key",
            "date_key",
            "quantity",
            "sales_amount",
        ],
    ),
    "dim_product": (
        dim_product,
        [
            "product_key",
            "product_id",
            "product_name",
            "category",
        ],
    ),
    "dim_customer": (
        dim_customer,
        [
            "customer_key",
            "customer_id",
            "customer_name",
            "city",
        ],
    ),
    "dim_date": (
        dim_date,
        [
            "date_key",
            "full_date",
        ],
    ),
}

for table_name, (df, columns) in required_columns.items():
    missing = [column for column in columns if column not in df.columns]

    if missing:
        raise ValueError(
            f"{table_name} is missing columns: {missing}"
        )


# ============================================================
# 6. CREATE LOCAL SQL ENGINE
# ============================================================

con = duckdb.connect(database=":memory:")

con.register("customer_df", dim_customer.to_arrow())
con.register("product_df", dim_product.to_arrow())
con.register("date_df", dim_date.to_arrow())
con.register("sales_df", fact_sales.to_arrow())

con.execute("CREATE TABLE dim_customer AS SELECT * FROM customer_df")
con.execute("CREATE TABLE dim_product AS SELECT * FROM product_df")
con.execute("CREATE TABLE dim_date AS SELECT * FROM date_df")
con.execute("CREATE TABLE fact_sales AS SELECT * FROM sales_df")

print("\nLocal SQL tables created successfully.")


# ============================================================
# 7. HELPER FUNCTION
# ============================================================

def run_query(title: str, sql: str, filename: str):

    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)

    result = con.execute(sql).pl()

    print(f"Result rows: {result.height}")

    if result.is_empty():
        print("No records found.")
    else:
        print(result.head(20))

    output_path = OUTPUT_FOLDER / filename
    result.write_csv(output_path)

    print(f"Saved: {output_path}")

    return result


# ============================================================
# QUESTION 1
# TOP 3 PRODUCTS BY REVENUE PER CATEGORY PER MONTH
# Concepts: JOIN, CTE, SUM, ROW_NUMBER
# ============================================================

q1 = """
WITH monthly_product_revenue AS (
    SELECT
        DATE_TRUNC('month', CAST(d.full_date AS DATE)) AS sales_month,
        p.category,
        p.product_key,
        p.product_name,
        SUM(f.sales_amount) AS revenue
    FROM fact_sales f
    INNER JOIN dim_product p
        ON f.product_key = p.product_key
    INNER JOIN dim_date d
        ON f.date_key = d.date_key
    GROUP BY
        DATE_TRUNC('month', CAST(d.full_date AS DATE)),
        p.category,
        p.product_key,
        p.product_name
),
ranked_products AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY sales_month, category
            ORDER BY revenue DESC, product_name
        ) AS product_rank
    FROM monthly_product_revenue
)
SELECT
    sales_month,
    category,
    product_name,
    ROUND(revenue, 2) AS revenue,
    product_rank
FROM ranked_products
WHERE product_rank <= 3
ORDER BY sales_month, category, product_rank
"""

q1_result = run_query(
    "QUESTION 1 - TOP 3 PRODUCTS PER CATEGORY PER MONTH",
    q1,
    "Q1_top3_products_per_category_month.csv",
)


# ============================================================
# QUESTION 2
# TOP 10 PRODUCTS BY TOTAL REVENUE
# Concepts: JOIN, GROUP BY, SUM, ORDER BY
# ============================================================

q2 = """
SELECT
    p.product_id,
    p.product_name,
    p.category,
    ROUND(SUM(f.sales_amount), 2) AS total_revenue,
    SUM(f.quantity) AS units_sold
FROM fact_sales f
INNER JOIN dim_product p
    ON f.product_key = p.product_key
GROUP BY
    p.product_id,
    p.product_name,
    p.category
ORDER BY total_revenue DESC
LIMIT 10
"""

q2_result = run_query(
    "QUESTION 2 - TOP 10 PRODUCTS BY REVENUE",
    q2,
    "Q2_top10_products.csv",
)


# ============================================================
# QUESTION 3
# MONTH-OVER-MONTH REVENUE
# Concepts: CTE, aggregation, LAG
# ============================================================

q3 = """
WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', CAST(d.full_date AS DATE)) AS sales_month,
        SUM(f.sales_amount) AS revenue
    FROM fact_sales f
    INNER JOIN dim_date d
        ON f.date_key = d.date_key
    GROUP BY 1
),
revenue_comparison AS (
    SELECT
        sales_month,
        revenue,
        LAG(revenue) OVER (
            ORDER BY sales_month
        ) AS previous_month_revenue
    FROM monthly_revenue
)
SELECT
    sales_month,
    ROUND(revenue, 2) AS revenue,
    ROUND(previous_month_revenue, 2) AS previous_month_revenue,
    ROUND(
        100.0 * (revenue - previous_month_revenue)
        / NULLIF(previous_month_revenue, 0),
        2
    ) AS growth_percent
FROM revenue_comparison
ORDER BY sales_month
"""

q3_result = run_query(
    "QUESTION 3 - MONTH OVER MONTH REVENUE",
    q3,
    "Q3_monthly_revenue.csv",
)


# ============================================================
# QUESTION 4
# CUSTOMERS WITHOUT ORDERS IN LAST 90 DAYS
# Reference date: latest date in the sales dataset
# Concepts: CTE, NOT EXISTS, date filtering
# ============================================================

q4 = """
WITH latest_sales_date AS (
    SELECT MAX(CAST(d.full_date AS DATE)) AS max_date
    FROM fact_sales f
    INNER JOIN dim_date d
        ON f.date_key = d.date_key
)
SELECT
    c.customer_key,
    c.customer_id,
    c.customer_name,
    c.city
FROM dim_customer c
CROSS JOIN latest_sales_date l
WHERE
    ('is_current' NOT IN (
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'dim_customer'
    ) OR c.is_current = TRUE)
    AND NOT EXISTS (
        SELECT 1
        FROM fact_sales f
        INNER JOIN dim_date d
            ON f.date_key = d.date_key
        WHERE f.customer_key = c.customer_key
          AND CAST(d.full_date AS DATE) > l.max_date - INTERVAL 90 DAY
          AND CAST(d.full_date AS DATE) <= l.max_date
    )
ORDER BY c.customer_id
"""

# Use a simpler query if your dim_customer table has no is_current column.
if "is_current" not in dim_customer.columns:
    q4 = """
    WITH latest_sales_date AS (
        SELECT MAX(CAST(full_date AS DATE)) AS max_date
        FROM dim_date
    )
    SELECT
        c.customer_key,
        c.customer_id,
        c.customer_name,
        c.city
    FROM dim_customer c
    CROSS JOIN latest_sales_date l
    WHERE NOT EXISTS (
        SELECT 1
        FROM fact_sales f
        INNER JOIN dim_date d
            ON f.date_key = d.date_key
        WHERE f.customer_key = c.customer_key
          AND CAST(d.full_date AS DATE) > l.max_date - INTERVAL 90 DAY
          AND CAST(d.full_date AS DATE) <= l.max_date
    )
    ORDER BY c.customer_id
    """
else:
    q4 = """
    WITH latest_sales_date AS (
        SELECT MAX(CAST(d.full_date AS DATE)) AS max_date
        FROM fact_sales f
        INNER JOIN dim_date d ON f.date_key = d.date_key
    )
    SELECT c.customer_key, c.customer_id, c.customer_name, c.city
    FROM dim_customer c
    CROSS JOIN latest_sales_date l
    WHERE c.is_current = TRUE
      AND NOT EXISTS (
          SELECT 1
          FROM fact_sales f
          INNER JOIN dim_date d ON f.date_key = d.date_key
          WHERE f.customer_key = c.customer_key
            AND CAST(d.full_date AS DATE) > l.max_date - INTERVAL 90 DAY
            AND CAST(d.full_date AS DATE) <= l.max_date
      )
    ORDER BY c.customer_id
    """

q4_result = run_query(
    "QUESTION 4 - CUSTOMERS WITHOUT RECENT ORDERS",
    q4,
    "Q4_inactive_customers.csv",
)


# ============================================================
# QUESTION 5
# QUERY PLAN AND DATE FILTERING
# Concepts: EXPLAIN, filter pushdown, query planning
# ============================================================

q5 = """
SELECT
    p.category,
    SUM(f.sales_amount) AS revenue
FROM fact_sales f
INNER JOIN dim_product p
    ON f.product_key = p.product_key
INNER JOIN dim_date d
    ON f.date_key = d.date_key
WHERE CAST(d.full_date AS DATE) >= DATE '2026-01-01'
  AND CAST(d.full_date AS DATE) < DATE '2026-04-01'
GROUP BY p.category
"""

print("\n" + "=" * 100)
print("QUESTION 5 - SQL QUERY EXECUTION PLAN")
print("=" * 100)

plan = con.execute("EXPLAIN " + q5).fetchall()
plan_text = "\n".join(str(row) for row in plan)
print(plan_text)

plan_path = OUTPUT_FOLDER / "Q5_query_execution_plan.txt"
plan_path.write_text(plan_text, encoding="utf-8")
print(f"Saved: {plan_path}")

q5_result = run_query(
    "QUESTION 5 - REVENUE FOR A DATE RANGE",
    q5,
    "Q5_date_filtered_revenue.csv",
)


# ============================================================
# 8. TASK 13 SUMMARY
# ============================================================

print("\n" + "=" * 100)
print("TASK 13 - SQL OPTIMIZATION COMPLETED")
print("=" * 100)

print(f"""
Q1 - Top 3 products per category per month: {q1_result.height} rows
Q2 - Top 10 products by revenue:            {q2_result.height} rows
Q3 - Month-over-month revenue:              {q3_result.height} rows
Q4 - Customers without recent orders:       {q4_result.height} rows
Q5 - Date-filtered revenue:                 {q5_result.height} rows

Output folder:
{OUTPUT_FOLDER}

SQL concepts demonstrated:
- INNER JOIN
- CTEs
- SUM and GROUP BY
- ROW_NUMBER and LAG window functions
- NOT EXISTS
- Date-range filtering
- EXPLAIN query plan
""")

print("=" * 100)
print("ALL SQL QUERIES COMPLETED")
print("=" * 100)

con.close()