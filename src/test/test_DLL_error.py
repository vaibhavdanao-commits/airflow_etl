from pathlib import Path

from pyiceberg.catalog import load_catalog


WAREHOUSE_FOLDER = Path(
    r"D:\airflow_etl\Warehouse"
)

CATALOG_DB = (
    WAREHOUSE_FOLDER /
    "pyiceberg_catalog.db"
)

WAREHOUSE_URI = (
    WAREHOUSE_FOLDER
    .resolve()
    .as_uri()
)


print("=" * 80)
print("TESTING PYICEBERG WITH FSSPEC FILEIO")
print("=" * 80)

print(
    f"Warehouse : {WAREHOUSE_FOLDER}"
)

print(
    f"Catalog DB: {CATALOG_DB}"
)

print(
    f"Warehouse URI: {WAREHOUSE_URI}"
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


print()
print("Catalog loaded successfully.")

print()
print(
    "Creating test schema..."
)

from pyiceberg.schema import Schema

from pyiceberg.types import (
    StringType,
    LongType,
    NestedField,
)


schema = Schema(
    NestedField(
        field_id=1,
        name="id",
        field_type=LongType(),
        required=False,
    ),
    NestedField(
        field_id=2,
        name="name",
        field_type=StringType(),
        required=False,
    ),
)


identifier = "default.test_fsspec"


print()
print(
    f"Test table: {identifier}"
)


if catalog.table_exists(identifier):

    print(
        "Test table already exists."
    )

    table = catalog.load_table(
        identifier
    )

else:

    print(
        "Creating test table..."
    )

    table = catalog.create_table(
        identifier=identifier,
        schema=schema
    )


print()
print("=" * 80)
print("SUCCESS")
print("=" * 80)

print()
print(
    "Iceberg table created successfully."
)

print(
    table
)