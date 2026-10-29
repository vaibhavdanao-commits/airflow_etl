from pathlib import Path
from pyiceberg.catalog import load_catalog

# ============================================================
# ICEBERG CONFIGURATION
# ============================================================

ICEBERG_CATALOG_NAME = "dimensional_model"

ICEBERG_CATALOG_DB = Path(
    r"D:\airflow_etl\Warehouse\dimensional_model_catalog.db"
)

ICEBERG_WAREHOUSE = Path(
    r"D:\airflow_etl\Warehouse\dimensional_model"
)

ICEBERG_NAMESPACE = "star_schema"


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 80)
    print("ICEBERG TABLE AND COLUMN INSPECTOR")
    print("=" * 80)

    print(f"Catalog Name : {ICEBERG_CATALOG_NAME}")
    print(f"Catalog DB   : {ICEBERG_CATALOG_DB}")
    print(f"Warehouse    : {ICEBERG_WAREHOUSE}")
    print(f"Namespace    : {ICEBERG_NAMESPACE}")

    # Validate catalog path
    if not ICEBERG_CATALOG_DB.is_file():
        raise FileNotFoundError(
            f"\nCatalog database not found:\n"
            f"{ICEBERG_CATALOG_DB}\n\n"
            "Verify that the configured path is correct."
        )

    # Load existing Iceberg catalog
    catalog = load_catalog(
        ICEBERG_CATALOG_NAME,
        type="sql",
        uri=f"sqlite:///{ICEBERG_CATALOG_DB.as_posix()}",
        warehouse=ICEBERG_WAREHOUSE.as_posix(),
        **{
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
        },
    )

    print("\n" + "=" * 80)
    print(f"SCHEMA / NAMESPACE: {ICEBERG_NAMESPACE}")
    print("=" * 80)

    # List tables only from the configured namespace
    namespace = (ICEBERG_NAMESPACE,)
    tables = catalog.list_tables(namespace)

    if not tables:
        print("\nNo tables found in this namespace.")
        print("Check that the namespace and catalog are correct.")
        return

    print(f"\nTotal tables found: {len(tables)}")

    # Inspect every table
    for table_number, identifier in enumerate(tables, start=1):
        table_name = ".".join(identifier)

        print("\n" + "-" * 80)
        print(f"TABLE {table_number}: {table_name}")
        print("-" * 80)

        try:
            table = catalog.load_table(identifier)
            schema = table.schema()

            print(f"Total columns: {len(schema.fields)}")
            print(f"{'COLUMN NAME':<35} {'DATA TYPE':<25} {'REQUIRED'}")
            print("-" * 80)

            for field in schema.fields:
                required = "YES" if field.required else "NO"

                print(
                    f"{field.name:<35} "
                    f"{str(field.field_type):<25} "
                    f"{required}"
                )

        except Exception as exc:
            print(f"Could not inspect table: {exc}")

    print("\n" + "=" * 80)
    print("SCHEMA INSPECTION COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    main()
