from __future__ import annotations

from pathlib import Path
import polars as pl
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType, DateType, DoubleType, FloatType, IntegerType,
    LongType, StringType, TimestampType, NestedField,
)


_THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = next(
    (p for p in _THIS_FILE.parents if (p / "Warehouse").exists() and (p / "curated").exists()),
    _THIS_FILE.parent,
)
WAREHOUSE_FOLDER = PROJECT_ROOT / "Warehouse"
CATALOG_DB = WAREHOUSE_FOLDER / "dimensional_model_catalog.db"
DIMENSIONAL_WAREHOUSE = WAREHOUSE_FOLDER / "dimensional_model"
WAREHOUSE_FOLDER.mkdir(parents=True, exist_ok=True)
DIMENSIONAL_WAREHOUSE.mkdir(parents=True, exist_ok=True)
WAREHOUSE_URI = DIMENSIONAL_WAREHOUSE.resolve().as_uri()
CATALOG_NAME = "dimensional_model"
NAMESPACE = "star_schema"

# Add actual mappings here if region is not available in curated/orders or dim_store.
# Example: REGION_BY_STORE = {"STORE_001": "West", "STORE_002": "South"}
REGION_BY_STORE: dict[str, str] = {}

catalog = load_catalog(
    CATALOG_NAME,
    type="sql",
    uri=f"sqlite:///{CATALOG_DB}",
    warehouse=WAREHOUSE_URI,
    **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"},
)


def table_id(table_name: str) -> str:
    return f"{NAMESPACE}.{table_name}"


def read_iceberg(table_name: str) -> pl.DataFrame:
    name = table_id(table_name)
    if not catalog.table_exists(name):
        raise RuntimeError(
            f"Required Iceberg table '{name}' was not found. Run your dimensional-model script first."
        )
    arrow_table = catalog.load_table(name).scan().to_arrow()
    return pl.from_arrow(arrow_table) if arrow_table.num_rows else pl.DataFrame()


def iceberg_type(dtype):
    if dtype == pl.Boolean:
        return BooleanType()
    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.UInt8, pl.UInt16):
        return IntegerType()
    if dtype in (pl.Int64, pl.UInt32, pl.UInt64):
        return LongType()
    if dtype == pl.Float32:
        return FloatType()
    if dtype == pl.Float64:
        return DoubleType()
    if dtype == pl.Date:
        return DateType()
    if isinstance(dtype, pl.Datetime):
        return TimestampType()
    return StringType()


def write_iceberg(table_name: str, df: pl.DataFrame) -> None:
    """Full-refresh one mart table. Only the named mart table is replaced."""
    if df.is_empty():
        raise ValueError(f"Refusing to write empty mart: {table_id(table_name)}")

    name = table_id(table_name)
    fields = [
        NestedField(
            field_id=i,
            name=column,
            field_type=iceberg_type(df[column].dtype),
            required=False,
        )
        for i, column in enumerate(df.columns, start=1)
    ]
    schema = Schema(*fields)

    if catalog.table_exists(name):
        catalog.drop_table(name)

    table = catalog.create_table(identifier=name, schema=schema)
    table.append(df.to_arrow())
    print(f"CREATED {name}: {df.height:,} rows; columns={df.columns}")


def normalize_date(df: pl.DataFrame, column: str) -> pl.DataFrame:
    if column not in df.columns:
        raise ValueError(f"Required date column '{column}' missing. Available: {df.columns}")
    dtype = df[column].dtype
    if dtype == pl.Date:
        return df
    if isinstance(dtype, pl.Datetime):
        return df.with_columns(pl.col(column).cast(pl.Date))
    return df.with_columns(pl.col(column).cast(pl.String).str.to_date(strict=False).alias(column))


def build_region_lookup(dim_store: pl.DataFrame) -> pl.DataFrame:
    """Use store region if present, configured mapping next, store_id as fallback."""
    if "store_id" not in dim_store.columns:
        raise ValueError("dim_store must contain store_id")

    stores = dim_store.select(pl.col("store_id").cast(pl.String)).unique()
    if "region" in dim_store.columns:
        return dim_store.select(
            pl.col("store_id").cast(pl.String),
            pl.col("region").cast(pl.String).fill_null("Unknown").alias("region"),
        ).unique(subset=["store_id"], keep="last")

    if REGION_BY_STORE:
        mapping = pl.DataFrame({
            "store_id": list(REGION_BY_STORE.keys()),
            "region": list(REGION_BY_STORE.values()),
        }).with_columns(pl.col("store_id").cast(pl.String), pl.col("region").cast(pl.String))
        stores = stores.join(mapping, on="store_id", how="left")
        return stores.with_columns(pl.col("region").fill_null("Unknown"))

    print("WARNING: no region column/mapping found. Using store_id as region placeholder.")
    print("         Configure REGION_BY_STORE in this script for true regional reporting.")
    return stores.with_columns(pl.col("store_id").alias("region"))


def create_sales_summary(fact, date_dim, product_dim, store_dim, region_lookup):
    required_fact = {"sale_id", "date_key", "product_key", "store_key", "quantity", "sales_amount"}
    missing = required_fact - set(fact.columns)
    if missing:
        raise ValueError(f"fact_sales missing columns: {sorted(missing)}")

    dates = date_dim.select(
        pl.col("date_key").cast(pl.Int32),
        pl.col("full_date").cast(pl.Date).alias("date"),
    )
    products = product_dim.select(
        pl.col("product_key").cast(pl.Int64),
        pl.col("category").cast(pl.String).fill_null("Unknown").alias("category"),
    )
    stores = store_dim.select(
        pl.col("store_key").cast(pl.Int64),
        pl.col("store_id").cast(pl.String),
    )
    sales = (
        fact.with_columns(
            pl.col("date_key").cast(pl.Int32),
            pl.col("product_key").cast(pl.Int64),
            pl.col("store_key").cast(pl.Int64),
            pl.col("sale_id").cast(pl.String),
            pl.col("quantity").cast(pl.Int64),
            pl.col("sales_amount").cast(pl.Float64),
        )
        .join(dates, on="date_key", how="left")
        .join(products, on="product_key", how="left")
        .join(stores, on="store_key", how="left")
        .join(region_lookup, on="store_id", how="left")
    )
    if sales.filter(pl.col("date").is_null()).height:
        raise ValueError("Some fact_sales rows did not match dim_date")

    return (
        sales.with_columns(
            pl.col("region").fill_null("Unknown"),
            pl.col("category").fill_null("Unknown"),
        )
        .group_by(["date", "region", "category"])
        .agg(
            pl.col("sale_id").n_unique().cast(pl.Int64).alias("total_orders"),
            pl.col("quantity").sum().alias("total_quantity"),
            pl.col("sales_amount").sum().round(2).alias("total_revenue"),
        )
        .select("date", "region", "category", "total_orders", "total_quantity", "total_revenue")
        .sort(["date", "region", "category"])
    )


def create_customer_analytics(fact, date_dim, customer_dim):
    required_fact = {"sale_id", "customer_key", "date_key", "sales_amount"}
    missing = required_fact - set(fact.columns)
    if missing:
        raise ValueError(f"fact_sales missing columns: {sorted(missing)}")
    required_customer = {"customer_key", "customer_id", "customer_segment"}
    missing = required_customer - set(customer_dim.columns)
    if missing:
        raise ValueError(f"dim_customer missing columns: {sorted(missing)}")

    dates = date_dim.select(
        pl.col("date_key").cast(pl.Int32),
        pl.col("full_date").cast(pl.Date).alias("order_date"),
    )
    # Map historical SCD2 customer keys to the natural customer_id first.
    # Use the current SCD2 version for the output customer_segment, so a
    # customer with a changed segment still produces exactly one mart row.
    customer_keys = customer_dim.select(
        pl.col("customer_key").cast(pl.Int64),
        pl.col("customer_id").cast(pl.String),
    ).unique(subset=["customer_key"])

    current_customers = customer_dim
    if "is_current" in current_customers.columns:
        current_customers = current_customers.filter(pl.col("is_current") == True)
    elif "effective_from" in current_customers.columns:
        current_customers = current_customers.sort("effective_from").unique(
            subset=["customer_id"], keep="last"
        )
    current_segments = current_customers.select(
        pl.col("customer_id").cast(pl.String),
        pl.col("customer_segment").cast(pl.String).fill_null("Unknown").alias("customer_segment"),
    ).unique(subset=["customer_id"], keep="last")

    sales = (
        fact.with_columns(
            pl.col("customer_key").cast(pl.Int64),
            pl.col("date_key").cast(pl.Int32),
            pl.col("sale_id").cast(pl.String),
            pl.col("sales_amount").cast(pl.Float64),
        )
        .join(customer_keys, on="customer_key", how="left")
        .join(dates, on="date_key", how="left")
    )
    result = (
        sales.filter(pl.col("customer_id").is_not_null())
        .group_by("customer_id")
        .agg(
            pl.col("sale_id").n_unique().cast(pl.Int64).alias("total_orders"),
            pl.col("sales_amount").sum().alias("total_spend"),
            pl.col("order_date").max().alias("last_order_date"),
        )
        .with_columns(
            pl.when(pl.col("total_orders") > 0)
            .then(pl.col("total_spend") / pl.col("total_orders"))
            .otherwise(0.0)
            .round(2)
            .alias("average_order_value"),
            pl.col("total_spend").round(2),
        )
        .join(current_segments, on="customer_id", how="left")
        .with_columns(pl.col("customer_segment").fill_null("Unknown"))
        .select(
            "customer_id", "total_orders", "total_spend",
            "average_order_value", "last_order_date", "customer_segment",
        )
        .sort("total_spend", descending=True)
    )
    if result.is_empty():
        raise ValueError("Customer mart has no matched customers. Check customer_key joins.")
    return result


def create_product_analytics(fact, product_dim):
    required_fact = {"sale_id", "product_key", "quantity", "sales_amount"}
    missing = required_fact - set(fact.columns)
    if missing:
        raise ValueError(f"fact_sales missing columns: {sorted(missing)}")
    required_product = {"product_key", "product_id"}
    missing = required_product - set(product_dim.columns)
    if missing:
        raise ValueError(f"dim_product missing columns: {sorted(missing)}")

    products = product_dim.select(
        pl.col("product_key").cast(pl.Int64),
        pl.col("product_id").cast(pl.String),
    )
    sales = (
        fact.with_columns(
            pl.col("product_key").cast(pl.Int64),
            pl.col("quantity").cast(pl.Int64),
            pl.col("sales_amount").cast(pl.Float64),
        )
        .join(products, on="product_key", how="left")
    )
    result = (
        sales.filter(pl.col("product_id").is_not_null())
        .group_by("product_id")
        .agg(
            pl.col("quantity").sum().cast(pl.Int64).alias("total_quantity_sold"),
            pl.col("sales_amount").sum().alias("total_revenue"),
        )
        .with_columns(
            pl.when(pl.col("total_quantity_sold") != 0)
            .then(pl.col("total_revenue") / pl.col("total_quantity_sold"))
            .otherwise(0.0)
            .round(2)
            .alias("average_price"),
            pl.col("total_revenue").round(2),
        )
        .with_columns(
            pl.col("total_revenue").rank(method="dense", descending=True)
            .cast(pl.Int64).alias("sales_rank")
        )
        .select("product_id", "total_quantity_sold", "total_revenue", "average_price", "sales_rank")
        .sort(["sales_rank", "product_id"])
    )
    if result.is_empty():
        raise ValueError("Product mart has no matched products. Check product_key joins.")
    return result


def main():
    print("=" * 78)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Catalog DB  : {CATALOG_DB}")
    print(f"Namespace   : {NAMESPACE}")
    print("=" * 78)

    fact = read_iceberg("fact_sales")
    customers = read_iceberg("dim_customer")
    products = read_iceberg("dim_product")
    stores = read_iceberg("dim_store")
    dates = read_iceberg("dim_date")

    if any(df.is_empty() for df in (fact, customers, products, stores, dates)):
        raise ValueError("One or more dimensional tables are empty. Rebuild the star schema first.")

    region_lookup = build_region_lookup(stores)
    marts = {
        "mart_sales_summary": create_sales_summary(fact, dates, products, stores, region_lookup),
        "mart_customer_analytics": create_customer_analytics(fact, dates, customers),
        "mart_product_analytics": create_product_analytics(fact, products),
    }

    # Each output is a managed Iceberg table in the existing catalog/namespace.
    for name, df in marts.items():
        write_iceberg(name, df)
        print(df.head(10))

    print("\nTASK 17 COMPLETED")
    print("Created Iceberg tables:")
    for name, df in marts.items():
        print(f"  {table_id(name)}: {df.height:,} rows")
    print("Note: this script full-refreshes only the three mart tables; it does not modify dimensions or fact_sales.")


if __name__ == "__main__":
    main()
