from pathlib import Path
from datetime import date, timedelta

import polars as pl
import pyarrow as pa

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



PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

CURATED_FOLDER = PROJECT_ROOT / "curated"

WAREHOUSE_FOLDER = PROJECT_ROOT / "Warehouse"

WAREHOUSE_FOLDER.mkdir(
    parents=True,
    exist_ok=True,
)



CATALOG_DB = (
    WAREHOUSE_FOLDER
    / "dimensional_model_catalog.db"
)

DIMENSIONAL_WAREHOUSE_FOLDER = (
    WAREHOUSE_FOLDER
    / "dimensional_model"
)

DIMENSIONAL_WAREHOUSE_FOLDER.mkdir(
    parents=True,
    exist_ok=True,
)

WAREHOUSE_URI = (
    DIMENSIONAL_WAREHOUSE_FOLDER
    .resolve()
    .as_uri()
)

NAMESPACE = "star_schema"


# =============================================================================
# SCD2 DATES
# =============================================================================

RUN_DATE = date.today()

MAX_DATE = date(9999, 12, 31)


# =============================================================================
# ICEBERG CATALOG
# =============================================================================

catalog = load_catalog(
    "dimensional_model",
    type="sql",
    uri=f"sqlite:///{CATALOG_DB}",
    warehouse=WAREHOUSE_URI,
    **{
        "py-io-impl":
        "pyiceberg.io.fsspec.FsspecFileIO"
    },
)


# =============================================================================
# CREATE NAMESPACE
# =============================================================================

try:

    catalog.create_namespace(
        NAMESPACE
    )

    print(
        f"Namespace created: {NAMESPACE}"
    )

except Exception as exc:

    message = str(exc).lower()

    if (
        "already exists" in message
        or "alreadyexists" in message
        or "namespacealreadyexists" in message
    ):

        pass

    else:

        raise


# =============================================================================
# GENERAL HELPERS
# =============================================================================

def identifier(table_name):

    return (
        f"{NAMESPACE}.{table_name}"
    )


def require_columns(
    df,
    columns,
    dataset_name,
):

    missing = [
        column
        for column in columns
        if column not in df.columns
    ]

    if missing:

        raise ValueError(
            f"{dataset_name} is missing required "
            f"columns: {missing}\n"
            f"Available columns: {df.columns}"
        )


def prepare_dataframe(df):

    if df.is_empty():

        return df

    remove_columns = [
        column
        for column in df.columns
        if column.startswith("__index")
        or column.startswith("Unnamed:")
    ]

    if remove_columns:

        df = df.drop(
            remove_columns
        )

    for column in df.columns:

        if df[column].dtype == pl.Object:

            df = df.with_columns(
                pl.col(column)
                .cast(pl.String)
            )

        if df[column].dtype == pl.Null:

            df = df.with_columns(
                pl.col(column)
                .cast(pl.String)
            )

    return df


def find_parquet_files(folder):

    if not folder.exists():

        return []

    return sorted(
        folder.rglob("*.parquet")
    )


# =============================================================================
# READ CURATED DATA
# =============================================================================

def read_all_curated(
    table_name
):

    folder = (
        CURATED_FOLDER
        / table_name
    )

    if not folder.exists():

        print(
            f"WARNING: curated folder not found: "
            f"{folder}"
        )

        return pl.DataFrame()

    paths = find_parquet_files(
        folder
    )

    if not paths:

        print(
            f"WARNING: no Parquet files found "
            f"for {table_name}"
        )

        return pl.DataFrame()

    print()
    print(
        f"Reading curated dataset: "
        f"{table_name}"
    )

    print(
        f"Files found: {len(paths)}"
    )

    frames = []

    for path in paths:

        print(
            f"  Reading: {path}"
        )

        df = prepare_dataframe(
            pl.read_parquet(path)
        )

        print(
            f"    Rows={df.height}, "
            f"Columns={df.width}"
        )

        if df.height > 0:

            frames.append(df)

    if not frames:

        return pl.DataFrame()

    if len(frames) == 1:

        return frames[0]

    return prepare_dataframe(
        pl.concat(
            frames,
            how="vertical_relaxed",
        )
    )


# =============================================================================
# DATE NORMALIZATION
# =============================================================================

def normalize_date_column(
    df,
    column,
):

    if column not in df.columns:

        return df

    dtype = df[column].dtype

    if dtype == pl.Date:

        return df

    if isinstance(
        dtype,
        pl.Datetime,
    ):

        return df.with_columns(
            pl.col(column)
            .cast(pl.Date)
        )

    if dtype == pl.String:

        return df.with_columns(
            pl.col(column)
            .str.strptime(
                pl.Date,
                strict=False,
            )
            .alias(column)
        )

    return df.with_columns(
        pl.col(column)
        .cast(pl.Date)
    )


# =============================================================================
# IMPORTANT:
# POLARS -> ARROW
# =============================================================================

def to_arrow(df):

    """
    Convert Polars directly to Arrow.

    DO NOT use:

        pa.Table.from_pandas(
            df.to_pandas()
        )

    because pandas can convert a Polars Date
    into timestamp[us].

    Polars native Arrow conversion preserves Date.
    """

    return df.to_arrow()


# =============================================================================
# POLARS -> ICEBERG TYPE
# =============================================================================

def iceberg_type(dtype):

    if dtype == pl.Boolean:

        return BooleanType()

    if dtype in (
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.UInt8,
        pl.UInt16,
    ):

        return IntegerType()

    if dtype in (
        pl.Int64,
        pl.UInt32,
        pl.UInt64,
    ):

        return LongType()

    if dtype == pl.Float32:

        return FloatType()

    if dtype == pl.Float64:

        return DoubleType()

    if dtype == pl.Date:

        return DateType()

    if isinstance(
        dtype,
        pl.Datetime,
    ):

        return TimestampType()

    return StringType()


# =============================================================================
# ICEBERG SCHEMA
# =============================================================================

def schema_from_dataframe(
    df
):

    fields = []

    for field_id, column in enumerate(
        df.columns,
        start=1,
    ):

        fields.append(
            NestedField(
                field_id=field_id,
                name=column,
                field_type=iceberg_type(
                    df[column].dtype
                ),
                required=False,
            )
        )

    return Schema(
        *fields
    )


# =============================================================================
# ICEBERG TABLE CHECK
# =============================================================================

def safe_table_exists(
    table_name
):

    name = identifier(
        table_name
    )

    try:

        return catalog.table_exists(
            name
        )

    except (
        FileNotFoundError,
        OSError,
    ) as exc:

        print()
        print(
            f"WARNING: broken metadata detected "
            f"for {name}"
        )

        print(
            f"Reason: {exc}"
        )

        return False

    except Exception as exc:

        print()
        print(
            f"WARNING: unable to check "
            f"{name}: {exc}"
        )

        return False


# =============================================================================
# READ ICEBERG TABLE
# =============================================================================

def read_iceberg(
    table_name
):

    name = identifier(
        table_name
    )

    try:

        if not catalog.table_exists(
            name
        ):

            return pl.DataFrame()

        table = catalog.load_table(
            name
        )

        arrow_table = (
            table
            .scan()
            .to_arrow()
        )

        if arrow_table.num_rows == 0:

            return pl.DataFrame()

        return pl.from_arrow(
            arrow_table
        )

    except (
        FileNotFoundError,
        OSError,
    ) as exc:

        print()
        print(
            f"WARNING: broken Iceberg table "
            f"detected: {name}"
        )

        print(
            f"Reason: {exc}"
        )

        return pl.DataFrame()


# =============================================================================
# DROP TABLE
# =============================================================================

def drop_table_safely(
    table_name
):

    name = identifier(
        table_name
    )

    try:

        if catalog.table_exists(
            name
        ):

            print(
                f"Dropping existing table: "
                f"{name}"
            )

            # IMPORTANT:
            #
            # DO NOT USE:
            #
            # catalog.drop_table(
            #     name,
            #     purge=True
            # )
            #
            # Your installed PyIceberg
            # SqlCatalog does not support
            # purge=True.
            #
            catalog.drop_table(
                name
            )

            print(
                f"Dropped: {name}"
            )

    except (
        FileNotFoundError,
        OSError,
    ) as exc:

        print(
            f"WARNING: stale metadata for "
            f"{name}: {exc}"
        )

    except Exception as exc:

        print(
            f"WARNING: could not drop "
            f"{name}: {exc}"
        )


# =============================================================================
# CREATE TABLE
# =============================================================================

def create_table(
    table_name,
    df,
):

    if df.is_empty():

        raise ValueError(
            f"Cannot create "
            f"{identifier(table_name)} "
            f"from empty DataFrame."
        )

    name = identifier(
        table_name
    )

    drop_table_safely(
        table_name
    )

    print()
    print(
        f"Creating Iceberg table: "
        f"{name}"
    )

    schema = (
        schema_from_dataframe(df)
    )

    table = catalog.create_table(
        identifier=name,
        schema=schema,
    )

    arrow_table = to_arrow(
        df
    )

    table.append(
        arrow_table
    )

    print(
        f"Created: {name}"
    )

    print(
        f"Rows: {df.height}"
    )

    return table


# =============================================================================
# REPLACE TABLE
# =============================================================================

def replace_table(
    table_name,
    df,
):

    if df.is_empty():

        raise ValueError(
            f"Cannot replace "
            f"{identifier(table_name)} "
            f"with empty data."
        )

    return create_table(
        table_name,
        df,
    )


# =============================================================================
# CUSTOMER SOURCE NORMALIZATION
# =============================================================================

def normalize_customer_name(
    df
):

    if "customer_name" in df.columns:

        return df

    if "name" in df.columns:

        return df.rename(
            {
                "name":
                "customer_name"
            }
        )

    raise ValueError(
        "Customer source must contain either "
        "'customer_name' or 'name'."
    )


# =============================================================================
# SCD2 COMPARISON
# =============================================================================

def normalize_for_compare(
    df,
    columns,
):

    if df.is_empty():

        return df

    expressions = []

    for column in columns:

        expressions.append(
            pl.col(column)
            .cast(pl.String)
            .fill_null("<NULL>")
            .alias(
                f"__cmp_{column}"
            )
        )

    return df.with_columns(
        expressions
    )


# =============================================================================
# SCD TYPE 2 CUSTOMER
# =============================================================================

def scd2_build_customer(
    source,
    initial_effective_from,
):
    """
    Customer SCD Type 2.

    Natural key:
        customer_id

    Surrogate key:
        customer_key

    Tracked attributes:
        customer_name
        email
        city
        state
        signup_date
        customer_segment

    INITIAL LOAD:
        effective_from =
        earliest order date

    NEW CUSTOMER AFTER INITIAL LOAD:
        effective_from =
        RUN_DATE

    CHANGED CUSTOMER:
        old version closed at
        RUN_DATE - 1

        new version starts at
        RUN_DATE
    """

    dimension_name = (
        "dim_customer"
    )

    natural_key = (
        "customer_id"
    )

    surrogate_key = (
        "customer_key"
    )

    attributes = [
        "customer_name",
        "email",
        "city",
        "state",
        "signup_date",
        "customer_segment",
    ]

    require_columns(
        source,
        [
            natural_key,
            *attributes,
        ],
        "customers",
    )

    source = source.select(
        [
            natural_key,
            *attributes,
        ]
    )

    source = source.filter(
        pl.col(natural_key)
        .is_not_null()
    )

    source = source.unique(
        subset=[
            natural_key
        ],
        keep="last",
    )

    source = normalize_date_column(
        source,
        "signup_date",
    )

    source = source.with_columns(
        pl.col(natural_key)
        .cast(pl.String)
        .alias(natural_key)
    )

    # =========================================================================
    # READ EXISTING DIMENSION
    # =========================================================================

    existing = read_iceberg(
        dimension_name
    )

    required_scd_columns = {
        surrogate_key,
        natural_key,
        *attributes,
        "effective_from",
        "effective_to",
        "is_current",
        "version",
    }

    # =========================================================================
    # INITIAL LOAD
    # =========================================================================

    if existing.is_empty():

        print()
        print(
            "dim_customer: creating initial "
            "SCD Type 2 version(s)."
        )

        initial = source.with_columns(

            pl.int_range(
                1,
                source.height + 1,
                eager=True,
            )
            .cast(pl.Int64)
            .alias(
                surrogate_key
            ),

            # IMPORTANT:
            # Use earliest business/order date,
            # NOT today's RUN_DATE.
            pl.lit(
                initial_effective_from
            )
            .cast(pl.Date)
            .alias(
                "effective_from"
            ),

            pl.lit(
                MAX_DATE
            )
            .cast(pl.Date)
            .alias(
                "effective_to"
            ),

            pl.lit(True)
            .cast(pl.Boolean)
            .alias(
                "is_current"
            ),

            pl.lit(1)
            .cast(pl.Int32)
            .alias(
                "version"
            ),
        )

        initial = initial.select(
            [
                surrogate_key,
                natural_key,
                *attributes,
                "effective_from",
                "effective_to",
                "is_current",
                "version",
            ]
        )

        initial = initial.with_columns(

            pl.col("customer_id")
            .cast(pl.String),

            pl.col("customer_name")
            .cast(pl.String),

            pl.col("email")
            .cast(pl.String),

            pl.col("city")
            .cast(pl.String),

            pl.col("state")
            .cast(pl.String),

            pl.col("signup_date")
            .cast(pl.Date),

            pl.col("customer_segment")
            .cast(pl.String),

            pl.col("customer_key")
            .cast(pl.Int64),

            pl.col("effective_from")
            .cast(pl.Date),

            pl.col("effective_to")
            .cast(pl.Date),

            pl.col("is_current")
            .cast(pl.Boolean),

            pl.col("version")
            .cast(pl.Int32),
        )

        replace_table(
            dimension_name,
            initial,
        )

        print()
        print(
            f"dim_customer: "
            f"{initial.height} initial row(s)"
        )

        print(
            f"Initial effective_from: "
            f"{initial_effective_from}"
        )

        return initial

    # =========================================================================
    # VALIDATE EXISTING TABLE
    # =========================================================================

    existing_columns = set(
        existing.columns
    )

    if not required_scd_columns.issubset(
        existing_columns
    ):

        missing = sorted(
            required_scd_columns
            - existing_columns
        )

        print()
        print(
            "WARNING: Existing "
            "dim_customer is not a valid "
            "SCD Type 2 table."
        )

        print(
            f"Missing columns: {missing}"
        )

        print(
            "Rebuilding dim_customer."
        )

        drop_table_safely(
            dimension_name
        )

        return scd2_build_customer(
            source,
            initial_effective_from,
        )

    # =========================================================================
    # NORMALIZE EXISTING DATA TYPES
    # =========================================================================

    existing = normalize_date_column(
        existing,
        "signup_date",
    )

    existing = normalize_date_column(
        existing,
        "effective_from",
    )

    existing = normalize_date_column(
        existing,
        "effective_to",
    )

    existing = existing.with_columns(

        pl.col("customer_id")
        .cast(pl.String),

        pl.col("customer_name")
        .cast(pl.String),

        pl.col("email")
        .cast(pl.String),

        pl.col("city")
        .cast(pl.String),

        pl.col("state")
        .cast(pl.String),

        pl.col("signup_date")
        .cast(pl.Date),

        pl.col("customer_segment")
        .cast(pl.String),

        pl.col("customer_key")
        .cast(pl.Int64),

        pl.col("effective_from")
        .cast(pl.Date),

        pl.col("effective_to")
        .cast(pl.Date),

        pl.col("is_current")
        .cast(pl.Boolean),

        pl.col("version")
        .cast(pl.Int32),
    )


    history_count = (
        existing
        .filter(
            pl.col("is_current")
            == False
        )
        .height
    )

    current_count = (
        existing
        .filter(
            pl.col("is_current")
            == True
        )
        .height
    )

    minimum_existing_effective_from = (
        existing[
            "effective_from"
        ].min()
    )

    if (
        history_count == 0
        and current_count > 0
        and minimum_existing_effective_from is not None
        and minimum_existing_effective_from
        > initial_effective_from
    ):

        print()
        print(
            "WARNING: Existing dim_customer "
            "was initialized too late."
        )

        print(
            f"Existing effective_from: "
            f"{minimum_existing_effective_from}"
        )

        print(
            f"Required initial date: "
            f"{initial_effective_from}"
        )

        print(
            "Rebuilding initial SCD2 "
            "customer dimension."
        )

        drop_table_safely(
            dimension_name
        )

        return scd2_build_customer(
            source,
            initial_effective_from,
        )

    # =========================================================================
    # CURRENT RECORDS
    # =========================================================================

    current = existing.filter(
        pl.col("is_current")
        == True
    )

    if current.is_empty():

        raise ValueError(
            "dim_customer exists but "
            "contains no current SCD2 records."
        )

    # =========================================================================
    # COMPARE CURRENT VS SOURCE
    # =========================================================================

    current_compare = (
        normalize_for_compare(
            current,
            attributes,
        )
    )

    source_compare = (
        normalize_for_compare(
            source,
            attributes,
        )
    )

    current_lookup = {
        row[natural_key]: row
        for row
        in current_compare.to_dicts()
    }

    source_rows = (
        source_compare.to_dicts()
    )

    max_existing_key = int(
        existing[
            surrogate_key
        ].max()
    )

    next_surrogate = (
        max_existing_key + 1
    )

    history_rows = (
        existing.to_dicts()
    )

    new_versions = []

    changed_count = 0

    new_customer_count = 0

    # =========================================================================
    # SCD2 PROCESSING
    # =========================================================================

    for src in source_rows:

        natural_value = (
            src[natural_key]
        )

        old = current_lookup.get(
            natural_value
        )

        # ---------------------------------------------------------------------
        # NEW CUSTOMER
        # ---------------------------------------------------------------------

        if old is None:

            new_row = {

                surrogate_key:
                    next_surrogate,

                natural_key:
                    natural_value,

                **{
                    column:
                        src[column]
                    for column
                    in attributes
                },

                "effective_from":
                    RUN_DATE,

                "effective_to":
                    MAX_DATE,

                "is_current":
                    True,

                "version":
                    1,
            }

            new_versions.append(
                new_row
            )

            next_surrogate += 1

            new_customer_count += 1

            continue

        # ---------------------------------------------------------------------
        # DETECT CHANGE
        # ---------------------------------------------------------------------

        changed = False

        for column in attributes:

            source_value = (
                src[column]
            )

            old_value = (
                old.get(column)
            )

            if str(source_value) != str(
                old_value
            ):

                changed = True

                break

        if not changed:

            continue

        changed_count += 1

        # ---------------------------------------------------------------------
        # SAME-DAY PROTECTION
        # ---------------------------------------------------------------------

        old_effective_from = (
            old.get(
                "effective_from"
            )
        )

        if (
            old_effective_from
            == RUN_DATE
        ):

            print()
            print(
                f"WARNING: customer "
                f"{natural_value} already "
                f"has a version starting "
                f"on {RUN_DATE}."
            )

            print(
                "Updating today's version "
                "instead of creating a "
                "zero-day version."
            )

            old_key = old[
                surrogate_key
            ]

            for row in history_rows:

                if (
                    row[
                        surrogate_key
                    ]
                    == old_key
                ):

                    for column in attributes:

                        row[column] = (
                            src[column]
                        )

                    row[
                        "effective_from"
                    ] = RUN_DATE

                    row[
                        "effective_to"
                    ] = MAX_DATE

                    row[
                        "is_current"
                    ] = True

            continue

        # ---------------------------------------------------------------------
        # CLOSE OLD VERSION
        # ---------------------------------------------------------------------

        old_key = old[
            surrogate_key
        ]

        for row in history_rows:

            if (
                row[
                    surrogate_key
                ]
                == old_key
            ):

                row[
                    "effective_to"
                ] = (
                    RUN_DATE
                    - timedelta(
                        days=1
                    )
                )

                row[
                    "is_current"
                ] = False

        # ---------------------------------------------------------------------
        # CREATE NEW VERSION
        # ---------------------------------------------------------------------

        old_version = int(
            old.get(
                "version"
            )
            or 1
        )

        new_row = {

            surrogate_key:
                next_surrogate,

            natural_key:
                natural_value,

            **{
                column:
                    src[column]
                for column
                in attributes
            },

            "effective_from":
                RUN_DATE,

            "effective_to":
                MAX_DATE,

            "is_current":
                True,

            "version":
                old_version + 1,
        }

        new_versions.append(
            new_row
        )

        next_surrogate += 1

    # =========================================================================
    # BUILD FINAL DATAFRAME
    # =========================================================================

    history_df = pl.DataFrame(
        history_rows
    )

    if new_versions:

        new_df = pl.DataFrame(
            new_versions
        )

        result = pl.concat(
            [
                history_df,
                new_df,
            ],
            how="vertical_relaxed",
        )

    else:

        result = history_df

    # =========================================================================
    # FINAL COLUMN ORDER
    # =========================================================================

    result = result.select(
        [
            surrogate_key,
            natural_key,
            *attributes,
            "effective_from",
            "effective_to",
            "is_current",
            "version",
        ]
    )

    # =========================================================================
    # FINAL TYPE NORMALIZATION
    # =========================================================================

    result = result.with_columns(

        pl.col("customer_key")
        .cast(pl.Int64),

        pl.col("customer_id")
        .cast(pl.String),

        pl.col("customer_name")
        .cast(pl.String),

        pl.col("email")
        .cast(pl.String),

        pl.col("city")
        .cast(pl.String),

        pl.col("state")
        .cast(pl.String),

        pl.col("signup_date")
        .cast(pl.Date),

        pl.col("customer_segment")
        .cast(pl.String),

        pl.col("effective_from")
        .cast(pl.Date),

        pl.col("effective_to")
        .cast(pl.Date),

        pl.col("is_current")
        .cast(pl.Boolean),

        pl.col("version")
        .cast(pl.Int32),
    )

    # =========================================================================
    # WRITE TABLE
    # =========================================================================

    replace_table(
        dimension_name,
        result,
    )

    print()

    print(
        f"dim_customer: "
        f"{new_customer_count} "
        f"new customer(s)"
    )

    print(
        f"dim_customer: "
        f"{changed_count} "
        f"changed customer(s)"
    )

    print(
        f"dim_customer total rows: "
        f"{result.height}"
    )

    return result


# =============================================================================
# DIM PRODUCT
# =============================================================================

def build_dim_product(
    products
):

    print()
    print("=" * 100)
    print("DIMENSION - dim_product")
    print("=" * 100)

    required = [
        "product_id",
        "product_name",
        "category",
        "price",
        "supplier",
    ]

    require_columns(
        products,
        required,
        "products",
    )

    products = products.select(
        required
    )

    products = products.filter(
        pl.col("product_id")
        .is_not_null()
    )

    products = products.unique(
        subset=[
            "product_id"
        ],
        keep="last",
    )

    products = products.with_columns(

        pl.col("product_id")
        .cast(pl.String),

        pl.col("product_name")
        .cast(pl.String),

        pl.col("category")
        .cast(pl.String),

        pl.col("price")
        .cast(pl.Float64),

        pl.col("supplier")
        .cast(pl.String),
    )

    products = products.with_row_index(
        "product_key",
        offset=1,
    )

    products = products.select(
        [
            pl.col("product_key")
            .cast(pl.Int64),

            "product_id",
            "product_name",
            "category",
            "price",
            "supplier",
        ]
    )

    replace_table(
        "dim_product",
        products,
    )

    print(
        f"dim_product: "
        f"{products.height} row(s)"
    )

    return products


# =============================================================================
# DIM STORE
# =============================================================================

def build_dim_store(
    orders
):

    print()
    print("=" * 100)
    print("DIMENSION - dim_store")
    print("=" * 100)

    require_columns(
        orders,
        [
            "store_id"
        ],
        "orders",
    )

    stores = (
        orders
        .select(
            pl.col("store_id")
            .cast(pl.String)
        )
        .drop_nulls()
        .unique()
        .sort("store_id")
    )

    if stores.is_empty():

        raise ValueError(
            "No store_id values found."
        )

    stores = stores.with_row_index(
        "store_key",
        offset=1,
    )

    stores = stores.select(
        [
            pl.col("store_key")
            .cast(pl.Int64),

            pl.col("store_id")
            .cast(pl.String),
        ]
    )

    replace_table(
        "dim_store",
        stores,
    )

    print(
        f"dim_store: "
        f"{stores.height} row(s)"
    )

    return stores


# =============================================================================
# DIM DATE
# =============================================================================

def build_dim_date(
    orders
):

    print()
    print("=" * 100)
    print("DIMENSION - dim_date")
    print("=" * 100)

    require_columns(
        orders,
        [
            "order_date"
        ],
        "orders",
    )

    orders = normalize_date_column(
        orders,
        "order_date",
    )

    dates = (
        orders
        .select(
            pl.col("order_date")
            .cast(pl.Date)
            .alias("full_date")
        )
        .drop_nulls()
        .unique()
        .sort("full_date")
    )

    if dates.is_empty():

        raise ValueError(
            "No valid order_date values found."
        )

    dates = dates.with_columns(

        pl.col("full_date")
        .dt.strftime(
            "%Y%m%d"
        )
        .cast(pl.Int32)
        .alias(
            "date_key"
        ),

        pl.col("full_date")
        .dt.year()
        .cast(pl.Int32)
        .alias(
            "year"
        ),

        (
            (
                pl.col("full_date")
                .dt.month()
                - 1
            )
            // 3
        )
        .add(1)
        .cast(pl.Int32)
        .alias(
            "quarter"
        ),

        pl.col("full_date")
        .dt.month()
        .cast(pl.Int32)
        .alias(
            "month"
        ),

        pl.col("full_date")
        .dt.strftime(
            "%B"
        )
        .alias(
            "month_name"
        ),

        pl.col("full_date")
        .dt.day()
        .cast(pl.Int32)
        .alias(
            "day"
        ),

        pl.col("full_date")
        .dt.weekday()
        .cast(pl.Int32)
        .alias(
            "day_of_week"
        ),

        pl.col("full_date")
        .dt.strftime(
            "%A"
        )
        .alias(
            "day_name"
        ),

        pl.col("full_date")
        .dt.week()
        .cast(pl.Int32)
        .alias(
            "week_of_year"
        ),
    )

    dates = dates.select(
        [
            "date_key",
            "full_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day",
            "day_of_week",
            "day_name",
            "week_of_year",
        ]
    )

    dates = dates.with_columns(
        pl.col("full_date")
        .cast(pl.Date)
    )

    replace_table(
        "dim_date",
        dates,
    )

    print(
        f"dim_date: "
        f"{dates.height} row(s)"
    )

    return dates


# =============================================================================
# FACT SALES
# =============================================================================

def build_fact_sales(
    orders,
    dim_customer,
    dim_product,
    dim_store,
    dim_date,
):

    print()
    print("=" * 100)
    print("FACT - fact_sales")
    print("=" * 100)

    required = [
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "unit_price",
        "store_id",
    ]

    require_columns(
        orders,
        required,
        "orders",
    )

    # =========================================================================
    # NORMALIZE ORDERS
    # =========================================================================

    orders = orders.with_columns(

        pl.col("order_id")
        .cast(pl.String),

        pl.col("customer_id")
        .cast(pl.String),

        pl.col("product_id")
        .cast(pl.String),

        pl.col("store_id")
        .cast(pl.String),

        pl.col("order_date")
        .cast(pl.Date)
        .alias("_order_date"),

        pl.col("quantity")
        .cast(pl.Int64),

        pl.col("unit_price")
        .cast(pl.Float64),
    )

    # =========================================================================
    # REMOVE DUPLICATE ORDERS
    # =========================================================================

    orders = orders.unique(
        subset=[
            "order_id"
        ],
        keep="last",
    )

    original_order_count = (
        orders.height
    )

    print(
        f"Unique orders: "
        f"{original_order_count}"
    )

    # =========================================================================
    # CUSTOMER SCD2 LOOKUP
    # =========================================================================

    print()
    print(
        "Applying customer SCD2 lookup..."
    )

    customer_lookup = (
        dim_customer
        .select(
            [
                "customer_key",
                "customer_id",
                "effective_from",
                "effective_to",
            ]
        )
    )

   

    orders_with_customer = (
        orders.join(
            customer_lookup,
            on="customer_id",
            how="left",
        )
    )

    # =========================================================================
    # APPLY SCD2 DATE RANGE
    # =========================================================================

    valid_customer_match = (
        pl.col("customer_key")
        .is_not_null()
        &
        (
            pl.col("_order_date")
            >= pl.col("effective_from")
        )
        &
        (
            pl.col("_order_date")
            <= pl.col("effective_to")
        )
    )

    orders_with_customer = (
        orders_with_customer
        .filter(
            valid_customer_match
        )
    )

    print(
        f"Orders after customer SCD2 lookup: "
        f"{orders_with_customer.height}"
    )

    # =========================================================================
    # CUSTOMER LOOKUP VALIDATION
    # =========================================================================

    if (
        orders_with_customer.height
        == 0
    ):

        print()
        print(
            "ERROR: No orders matched "
            "the customer SCD2 validity period."
        )

        print()
        print(
            "Customer effective date range:"
        )

        print(
            dim_customer.select(
                [
                    "effective_from",
                    "effective_to",
                ]
            ).unique()
        )

        print()
        print(
            "Order date range:"
        )

        print(
            orders.select(
                [
                    pl.col("_order_date")
                    .min()
                    .alias("min_order_date"),

                    pl.col("_order_date")
                    .max()
                    .alias("max_order_date"),
                ]
            )
        )

        raise ValueError(
            "No fact rows remain after "
            "customer SCD2 lookup."
        )

    # =========================================================================
    # PRODUCT LOOKUP
    # =========================================================================

    print(
        "Applying product lookup..."
    )

    product_lookup = (
        dim_product
        .select(
            [
                "product_key",
                "product_id",
            ]
        )
    )

    orders_with_customer = (
        orders_with_customer.join(
            product_lookup,
            on="product_id",
            how="left",
        )
    )

    # =========================================================================
    # STORE LOOKUP
    # =========================================================================

    print(
        "Applying store lookup..."
    )

    store_lookup = (
        dim_store
        .select(
            [
                "store_id",
                "store_key",
            ]
        )
    )

    orders_with_customer = (
        orders_with_customer.join(
            store_lookup,
            on="store_id",
            how="left",
        )
    )

    # =========================================================================
    # DATE LOOKUP
    # =========================================================================

    print(
        "Applying date lookup..."
    )

    date_lookup = (
        dim_date
        .select(
            [
                "date_key",
                "full_date",
            ]
        )
    )

    orders_with_customer = (
        orders_with_customer.join(
            date_lookup,
            left_on="_order_date",
            right_on="full_date",
            how="left",
        )
    )

    # =========================================================================
    # LOOKUP VALIDATION
    # =========================================================================

    missing_customer = (
        orders_with_customer
        .filter(
            pl.col("customer_key")
            .is_null()
        )
        .height
    )

    missing_product = (
        orders_with_customer
        .filter(
            pl.col("product_key")
            .is_null()
        )
        .height
    )

    missing_store = (
        orders_with_customer
        .filter(
            pl.col("store_key")
            .is_null()
        )
        .height
    )

    missing_date = (
        orders_with_customer
        .filter(
            pl.col("date_key")
            .is_null()
        )
        .height
    )

    print()
    print(
        f"Missing customer keys: "
        f"{missing_customer}"
    )

    print(
        f"Missing product keys : "
        f"{missing_product}"
    )

    print(
        f"Missing store keys   : "
        f"{missing_store}"
    )

    print(
        f"Missing date keys    : "
        f"{missing_date}"
    )

    if (
        missing_customer > 0
        or missing_product > 0
        or missing_store > 0
        or missing_date > 0
    ):

        print()
        print(
            "ERROR: Dimension lookup failed."
        )

        raise ValueError(
            "Fact load stopped because "
            "one or more dimension "
            "lookups failed."
        )

    # =========================================================================
    # DISCOUNT
    # =========================================================================

    if "discount" in orders_with_customer.columns:

        discount_expr = (
            pl.col("discount")
            .cast(pl.Float64)
            .fill_null(0.0)
        )

    else:

        discount_expr = (
            pl.lit(0.0)
            .cast(pl.Float64)
        )


    fact = (
        orders_with_customer
        .select(
            [
                pl.col("order_id")
                .cast(pl.String)
                .alias("sale_id"),

                pl.col("customer_key")
                .cast(pl.Int64),

                pl.col("product_key")
                .cast(pl.Int64),

                pl.col("store_key")
                .cast(pl.Int64),

                pl.col("date_key")
                .cast(pl.Int32),

                pl.col("quantity")
                .cast(pl.Int64),

                (
                    pl.col("quantity")
                    .cast(pl.Float64)
                    *
                    pl.col("unit_price")
                    .cast(pl.Float64)
                )
                .cast(pl.Float64)
                .alias(
                    "sales_amount"
                ),

                discount_expr
                .alias(
                    "discount"
                ),
            ]
        )
    )

    # =========================================================================
    # REMOVE DUPLICATE SALES
    # =========================================================================

    fact = fact.unique(
        subset=[
            "sale_id"
        ],
        keep="last",
    )

    # =========================================================================
    # FINAL TYPE NORMALIZATION
    # =========================================================================

    fact = fact.with_columns(

        pl.col("sale_id")
        .cast(pl.String),

        pl.col("customer_key")
        .cast(pl.Int64),

        pl.col("product_key")
        .cast(pl.Int64),

        pl.col("store_key")
        .cast(pl.Int64),

        pl.col("date_key")
        .cast(pl.Int32),

        pl.col("quantity")
        .cast(pl.Int64),

        pl.col("sales_amount")
        .cast(pl.Float64),

        pl.col("discount")
        .cast(pl.Float64),
    )

    # =========================================================================
    # EMPTY FACT PROTECTION
    # =========================================================================

    if fact.is_empty():

        raise ValueError(
            "fact_sales became empty after "
            "dimension lookups."
        )

    # =========================================================================
    # CREATE FACT TABLE
    # =========================================================================

    replace_table(
        "fact_sales",
        fact,
    )

    print()
    print(
        f"fact_sales: "
        f"{fact.height} row(s)"
    )

    return fact


# =============================================================================
# VERIFICATION
# =============================================================================

DIMENSIONAL_TABLES = [
    "dim_customer",
    "dim_product",
    "dim_store",
    "dim_date",
    "fact_sales",
]


def verify_dimensional_model():

    print()
    print("=" * 100)
    print("DIMENSIONAL MODEL VERIFICATION")
    print("=" * 100)

    print()
    print(
        f"Catalog DB:"
    )

    print(
        f"  {CATALOG_DB}"
    )

    print()
    print(
        f"Dimensional Warehouse:"
    )

    print(
        f"  {DIMENSIONAL_WAREHOUSE_FOLDER}"
    )

    # =========================================================================
    # TABLE VERIFICATION
    # =========================================================================

    for table_name in DIMENSIONAL_TABLES:

        name = identifier(
            table_name
        )

        print()
        print(
            "-" * 100
        )

        print(
            f"TABLE: {name}"
        )

        if not safe_table_exists(
            table_name
        ):

            print(
                "STATUS: NOT FOUND"
            )

            continue

        table = catalog.load_table(
            name
        )

        arrow_table = (
            table
            .scan()
            .to_arrow()
        )

        print(
            f"Rows: "
            f"{arrow_table.num_rows}"
        )

        print(
            f"Columns: "
            f"{arrow_table.num_columns}"
        )

        print(
            f"Schema:"
        )

        print(
            arrow_table.schema
        )

        try:

            snapshots = list(
                table.snapshots()
            )

            print(
                f"Snapshots: "
                f"{len(snapshots)}"
            )

        except Exception:

            print(
                "Snapshots: unavailable"
            )

    # =========================================================================
    # SCD2 VERIFICATION
    # =========================================================================

    print()
    print("=" * 100)
    print("SCD TYPE 2 VERIFICATION")
    print("=" * 100)

    customer_df = read_iceberg(
        "dim_customer"
    )

    if customer_df.is_empty():

        raise ValueError(
            "dim_customer contains no data."
        )

    current_count = (
        customer_df
        .filter(
            pl.col("is_current")
            == True
        )
        .height
    )

    historical_count = (
        customer_df
        .filter(
            pl.col("is_current")
            == False
        )
        .height
    )

    print()
    print(
        f"Total customer rows : "
        f"{customer_df.height}"
    )

    print(
        f"Current records     : "
        f"{current_count}"
    )

    print(
        f"Historical records : "
        f"{historical_count}"
    )

    # =========================================================================
    # DUPLICATE CURRENT CUSTOMER CHECK
    # =========================================================================

    duplicate_current = (
        customer_df
        .filter(
            pl.col("is_current")
            == True
        )
        .group_by(
            "customer_id"
        )
        .len()
        .filter(
            pl.col("len")
            > 1
        )
    )

    if duplicate_current.height > 0:

        print()
        print(
            "ERROR: Multiple current "
            "records found:"
        )

        print(
            duplicate_current
        )

        raise ValueError(
            "SCD2 validation failed."
        )

    print()
    print(
        "PASS: One current record "
        "per customer."
    )

    # =========================================================================
    # DATE RANGE VALIDATION
    # =========================================================================

    invalid_dates = (
        customer_df
        .filter(
            pl.col("effective_from")
            >
            pl.col("effective_to")
        )
    )

    if invalid_dates.height > 0:

        raise ValueError(
            "SCD2 validation failed: "
            "effective_from > effective_to."
        )

    print(
        "PASS: Effective date ranges "
        "are valid."
    )

    # =========================================================================
    # FACT VERIFICATION
    # =========================================================================

    fact_df = read_iceberg(
        "fact_sales"
    )

    if fact_df.is_empty():

        raise ValueError(
            "fact_sales contains no data."
        )

    print()
    print(
        f"fact_sales rows: "
        f"{fact_df.height}"
    )

    expected_fact_columns = [
        "sale_id",
        "customer_key",
        "product_key",
        "store_key",
        "date_key",
        "quantity",
        "sales_amount",
        "discount",
    ]

    if (
        fact_df.columns
        != expected_fact_columns
    ):

        raise ValueError(
            "fact_sales schema does not "
            "match the expected fact schema.\n"
            f"Expected: "
            f"{expected_fact_columns}\n"
            f"Actual: "
            f"{fact_df.columns}"
        )

    print()
    print(
        "PASS: fact_sales schema is correct."
    )

    # =========================================================================
    # FACT DUPLICATE CHECK
    # =========================================================================

    duplicate_sales = (
        fact_df.height
        -
        fact_df
        .select("sale_id")
        .unique()
        .height
    )

    if duplicate_sales > 0:

        raise ValueError(
            f"fact_sales contains "
            f"{duplicate_sales} duplicate sale_id values."
        )

    print(
        "PASS: No duplicate sale_id values."
    )


# =============================================================================
# STAR SCHEMA SUMMARY
# =============================================================================

def print_star_schema():

    print()
    print("=" * 100)
    print("STAR SCHEMA")
    print("=" * 100)

    print()
    print(
        """
                         dim_customer
                         ------------
                         customer_key
                              |
                              |
                              |
dim_product -----------> fact_sales <----------- dim_store
------------              ----------              ---------
product_key               sale_id                 store_key
product_id                customer_key            store_id
                          product_key
                          store_key
                          date_key
                          quantity
                          sales_amount
                          discount
                              |
                              |
                           dim_date
                           --------
                           date_key
                           full_date
"""
    )

    print(
        "DIMENSIONS:"
    )

    print(
        "  1. dim_customer  -> SCD Type 2"
    )

    print(
        "  2. dim_product"
    )

    print(
        "  3. dim_store"
    )

    print(
        "  4. dim_date"
    )

    print()
    print(
        "FACT:"
    )

    print(
        "  5. fact_sales"
    )

    print()
    print(
        "FACT COLUMNS:"
    )

    print(
        "  sale_id"
    )

    print(
        "  customer_key"
    )

    print(
        "  product_key"
    )

    print(
        "  store_key"
    )

    print(
        "  date_key"
    )

    print(
        "  quantity"
    )

    print(
        "  sales_amount"
    )

    print(
        "  discount"
    )


# =============================================================================
# MAIN
# =============================================================================

def main():

    print()
    print("=" * 100)
    print("TASK 7 + TASK 8")
    print("FULL STAR SCHEMA + SCD TYPE 2")
    print("=" * 100)

    print()
    print(
        f"Project root       : "
        f"{PROJECT_ROOT}"
    )

    print(
        f"Curated            : "
        f"{CURATED_FOLDER}"
    )

    print(
        f"Dimensional DB     : "
        f"{CATALOG_DB}"
    )

    print(
        f"Dimensional Store  : "
        f"{DIMENSIONAL_WAREHOUSE_FOLDER}"
    )

    print(
        f"Namespace          : "
        f"{NAMESPACE}"
    )

    print(
        f"SCD run date       : "
        f"{RUN_DATE}"
    )

    # =========================================================================
    # SOURCE VALIDATION
    # =========================================================================

    if not CURATED_FOLDER.exists():

        raise FileNotFoundError(
            f"Curated folder does not exist: "
            f"{CURATED_FOLDER}"
        )

    # =========================================================================
    # READ CURATED DATA
    # =========================================================================

    print()
    print("=" * 100)
    print("READING COMPLETE CURATED DATA")
    print("=" * 100)

    customers = read_all_curated(
        "customers"
    )

    products = read_all_curated(
        "products"
    )

    orders = read_all_curated(
        "orders"
    )

    if customers.is_empty():

        raise ValueError(
            "No customer data found."
        )

    if products.is_empty():

        raise ValueError(
            "No product data found."
        )

    if orders.is_empty():

        raise ValueError(
            "No order data found."
        )

    # =========================================================================
    # NORMALIZE ORDER DATE
    # =========================================================================

    orders = normalize_date_column(
        orders,
        "order_date",
    )

   
    initial_effective_from = (
        orders
        .select(
            pl.col("order_date")
            .cast(pl.Date)
            .min()
            .alias(
                "minimum_order_date"
            )
        )
        .item()
    )

    if initial_effective_from is None:

        raise ValueError(
            "Could not determine initial "
            "SCD2 effective date."
        )

    print()
    print(
        "=" * 100
    )

    print(
        "SCD2 INITIAL EFFECTIVE DATE"
    )

    print(
        "=" * 100
    )

    print()
    print(
        f"RUN_DATE: "
        f"{RUN_DATE}"
    )

    print(
        f"Earliest order date: "
        f"{initial_effective_from}"
    )

    print()
    print(
        "Initial customer versions "
        "will start from the earliest "
        "order date."
    )

    # =========================================================================
    # BUILD DIM CUSTOMER
    # =========================================================================

    customers = normalize_customer_name(
        customers
    )

    dim_customer = (
        scd2_build_customer(
            source=customers,
            initial_effective_from=(
                initial_effective_from
            ),
        )
    )

    # =========================================================================
    # BUILD DIM PRODUCT
    # =========================================================================

    dim_product = (
        build_dim_product(
            products
        )
    )

    # =========================================================================
    # BUILD DIM STORE
    # =========================================================================

    dim_store = (
        build_dim_store(
            orders
        )
    )

    # =========================================================================
    # BUILD DIM DATE
    # =========================================================================

    dim_date = (
        build_dim_date(
            orders
        )
    )

    # =========================================================================
    # BUILD FACT SALES
    # =========================================================================

    fact_sales = (
        build_fact_sales(

            orders=orders,

            dim_customer=(
                dim_customer
            ),

            dim_product=(
                dim_product
            ),

            dim_store=(
                dim_store
            ),

            dim_date=(
                dim_date
            ),
        )
    )

    # =========================================================================
    # VERIFY
    # =========================================================================

    verify_dimensional_model()

    # =========================================================================
    # STAR SCHEMA
    # =========================================================================

    print_star_schema()

    # =========================================================================
    # FINAL SUMMARY
    # =========================================================================

    print()
    print("=" * 100)
    print("TASK 7 + TASK 8 COMPLETED SUCCESSFULLY")
    print("=" * 100)

    print()
    print(
        "CATALOG:"
    )

    print(
        f"  {CATALOG_DB}"
    )

    print()
    print(
        "WAREHOUSE:"
    )

    print(
        f"  {DIMENSIONAL_WAREHOUSE_FOLDER}"
    )

    print()
    print(
        "NAMESPACE:"
    )

    print(
        f"  {NAMESPACE}"
    )

    print()
    print(
        "TABLES:"
    )

    print(
        "  1. star_schema.dim_customer"
    )

    print(
        "  2. star_schema.dim_product"
    )

    print(
        "  3. star_schema.dim_store"
    )

    print(
        "  4. star_schema.dim_date"
    )

    print(
        "  5. star_schema.fact_sales"
    )

    print()
    print(
        "ROW COUNTS:"
    )

    print(
        f"  dim_customer : "
        f"{dim_customer.height}"
    )

    print(
        f"  dim_product  : "
        f"{dim_product.height}"
    )

    print(
        f"  dim_store    : "
        f"{dim_store.height}"
    )

    print(
        f"  dim_date     : "
        f"{dim_date.height}"
    )

    print(
        f"  fact_sales   : "
        f"{fact_sales.height}"
    )
    print()
    print("=" * 100)


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":

    main()