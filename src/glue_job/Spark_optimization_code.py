
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("task14_spark_optimization")


ORDERS_COLUMNS = [
    "order_id", "customer_id", "product_id", "order_date",
    "quantity", "unit_price", "order_status",
]
CUSTOMER_COLUMNS = [
    "customer_id", "customer_name", "city", "state", "customer_segment",
]
PRODUCT_COLUMNS = ["product_id", "product_name", "category", "price"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Task 14 PySpark optimization demo")
    parser.add_argument("--orders", required=True, help="Orders CSV/Parquet file or directory")
    parser.add_argument("--customers", required=True, help="Customers CSV/Parquet file or directory")
    parser.add_argument("--products", required=True, help="Products CSV/Parquet file or directory")
    parser.add_argument("--format", choices=("parquet", "csv"), default="parquet")
    parser.add_argument("--output", default="task14_results")
    parser.add_argument("--start-date", default=None, help="Inclusive order date, e.g. 2026-01-01")
    parser.add_argument("--end-date", default=None, help="Exclusive order date, e.g. 2026-04-01")
    parser.add_argument(
        "--broadcast-product-max-mb",
        type=int,
        default=100,
        help="Broadcast product only if estimated serialized size is below this threshold; set 0 to disable.",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=max(200, int(os.cpu_count() or 4) * 2),
        help="Initial shuffle partition count; tune for the cluster and data volume.",
    )
    parser.add_argument(
        "--cache-filtered-orders",
        action="store_true",
        help="Cache filtered orders only if it is reused and memory/storage capacity is sufficient.",
    )
    parser.add_argument(
        "--output-partitions",
        type=int,
        default=200,
        help="Number of output partitions before writing; tune based on output size.",
    )
    return parser.parse_args()


def create_spark(app_name: str, shuffle_partitions: int) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # AQE can coalesce small shuffle partitions and optimize some join strategies at runtime.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate()
    )


def read_source(spark: SparkSession, path: str, file_format: str) -> DataFrame:
    reader = spark.read
    if file_format == "csv":
        return reader.option("header", True).option("inferSchema", True).csv(path)
    return reader.parquet(path)


def require_columns(df: DataFrame, required: list[str], label: str) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def load_and_project(
    spark: SparkSession,
    args: argparse.Namespace,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    # Column projection reduces data read, network transfer, and memory use.
    orders = read_source(spark, args.orders, args.format)
    customers = read_source(spark, args.customers, args.format)
    products = read_source(spark, args.products, args.format)

    require_columns(orders, ORDERS_COLUMNS, "Orders")
    require_columns(customers, CUSTOMER_COLUMNS, "Customers")
    require_columns(products, PRODUCT_COLUMNS, "Products")

    orders = orders.select(
        F.col("order_id").cast("string").alias("order_id"),
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("product_id").cast("string").alias("product_id"),
        F.to_date("order_date").alias("order_date"),
        F.col("quantity").cast("long").alias("quantity"),
        F.col("unit_price").cast("double").alias("unit_price"),
        F.col("order_status").cast("string").alias("order_status"),
    )
    customers = customers.select(
        F.col("customer_id").cast("string").alias("customer_id"),
        "customer_name", "city", "state", "customer_segment",
    )
    products = products.select(
        F.col("product_id").cast("string").alias("product_id"),
        "product_name", "category",
        F.col("price").cast("double").alias("product_price"),
    )
    return orders, customers, products


def apply_order_filters(
    orders: DataFrame,
    start_date: Optional[str],
    end_date: Optional[str],
) -> DataFrame:
    # Keep filters directly on the source column so Parquet/partition pruning
    # and predicate pushdown can work. For partition pruning, the source must
    # actually be partitioned by the relevant date column/path.
    if start_date:
        orders = orders.filter(F.col("order_date") >= F.to_date(F.lit(start_date)))
    if end_date:
        orders = orders.filter(F.col("order_date") < F.to_date(F.lit(end_date)))
    # Example business filter. Remove this line if cancelled orders are required.
    orders = orders.filter(F.col("order_status").isNotNull())
    return orders


def maybe_broadcast_products(
    products: DataFrame,
    max_mb: int,
) -> tuple[DataFrame, bool]:
    
    if max_mb <= 0:
        return products, False

    # Spark's autoBroadcastJoinThreshold and AQE can make a better choice when
    
    try:
        estimated_bytes = products._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
        estimated_mb = int(estimated_bytes) / (1024 * 1024)
        LOG.info("Spark estimate for products: %.2f MiB", estimated_mb)
        if estimated_bytes <= max_mb * 1024 * 1024:
            return broadcast(products), True
        LOG.warning(
            "Product estimate exceeds broadcast cap (%s MiB); using regular join.",
            max_mb,
        )
    except Exception as exc:
        LOG.warning("Could not obtain product size estimate (%s); using regular join.", exc)
    return products, False


def main() -> None:
    args = parse_args()
    spark = create_spark("Task14-PySpark-Optimization", args.shuffle_partitions)
    spark.sparkContext.setLogLevel("WARN")

    try:
        LOG.info("Reading and projecting source columns...")
        orders, customers, products = load_and_project(spark, args)

        LOG.info("Applying date/status filters before joins...")
        orders = apply_order_filters(orders, args.start_date, args.end_date)

        if args.cache_filtered_orders:
         
            orders = orders.persist()
            LOG.info("Filtered orders persisted because --cache-filtered-orders was specified.")


        product_join_side, product_is_broadcast = maybe_broadcast_products(
            products, args.broadcast_product_max_mb
        )
        LOG.info("Broadcast product dimension: %s", product_is_broadcast)

        
        joined = (
            orders.join(customers, on="customer_id", how="inner")
            .join(product_join_side, on="product_id", how="inner")
            .select(
                "order_id", "order_date", "customer_id", "customer_name",
                "city", "state", "customer_segment", "product_id",
                "product_name", "category", "quantity", "unit_price",
                "product_price", "order_status",
                (F.col("quantity") * F.col("unit_price")).alias("sales_amount"),
            )
        )

        # Inspect the physical plan: look for BroadcastHashJoin for Products
        LOG.info("Physical plan for optimized join:")
        joined.explain(mode="formatted")

        # Write columnar output. Avoid coalesce(1): it forces a single output
        # task and can create a bottleneck for a large result.
        output = args.output
        output_path = str(Path(output).resolve())
        write_df = joined
        if args.output_partitions > 0:
            write_df = joined.repartition(args.output_partitions, "order_date")
        (
            write_df.write
            .mode("overwrite")
            .option("compression", "snappy")
            .partitionBy("order_date")
            .parquet(output_path)
        )
        LOG.info("Optimized joined data written to %s", output_path)
        LOG.info("Review Spark UI/stage metrics to tune shuffle partitions and output file sizes.")

        if args.cache_filtered_orders:
            orders.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
