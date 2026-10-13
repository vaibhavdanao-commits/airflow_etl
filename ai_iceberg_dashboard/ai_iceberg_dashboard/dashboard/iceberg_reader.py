
from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import duckdb
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog


# ============================================================
# CONFIGURATION
# ============================================================

HERE = Path(__file__).resolve().parent
load_dotenv(HERE / ".env")


def get_config() -> dict[str, str]:
    """Load configuration for the existing Iceberg warehouse."""

    return {
        "catalog_name": os.getenv(
            "ICEBERG_CATALOG_NAME",
            "dimensional_model",
        ),
        "catalog_db": os.getenv(
            "ICEBERG_CATALOG_DB",
            r"D:\airflow_etl\Warehouse\dimensional_model_catalog.db",
        ),
        "warehouse": os.getenv(
            "ICEBERG_WAREHOUSE",
            r"D:\airflow_etl\Warehouse\dimensional_model",
        ),
        "namespace": os.getenv(
            "ICEBERG_NAMESPACE",
            "star_schema",
        ),
    }


def windows_file_uri_to_path(value: str) -> str:
    """
    Convert a Windows local file URI to a native Windows path.

    Example:
        file:///D:/folder/file.json
        -> D:\\folder\\file.json

    This helper does not modify catalog metadata.
    """
    parsed = urlparse(value)

    if parsed.scheme.lower() != "file":
        return value

    path = unquote(parsed.path)

    # Convert /D:/folder/file.json to D:/folder/file.json.
    if (
        len(path) >= 3
        and path.startswith("/")
        and path[2] == ":"
    ):
        path = path[1:]

    # Handle file://server/share paths if encountered.
    if parsed.netloc and parsed.netloc.lower() != "localhost":
        path = f"//{parsed.netloc}{path}"

    return str(Path(path))


# ============================================================
# ICEBERG CATALOG
# ============================================================

def get_catalog():
    """
    Connect to the existing SQL-backed Iceberg catalog.

    This function does not intentionally create a replacement
    catalog or namespace.
    """
    config = get_config()

    catalog_db = Path(config["catalog_db"]).expanduser().resolve()
    warehouse = Path(config["warehouse"]).expanduser().resolve()
    namespace = config["namespace"]

    if not catalog_db.is_file():
        raise FileNotFoundError(
            f"Iceberg catalog database was not found:\n{catalog_db}\n"
            "Check ICEBERG_CATALOG_DB in dashboard/.env."
        )

    if not warehouse.is_dir():
        raise FileNotFoundError(
            f"Iceberg warehouse directory was not found:\n{warehouse}\n"
            "Check ICEBERG_WAREHOUSE in dashboard/.env."
        )

    # SQLite URI for a Windows drive path.
    sqlite_uri = f"sqlite:///{catalog_db.as_posix()}"

    catalog = load_catalog(
        config["catalog_name"],
        type="sql",
        uri=sqlite_uri,
        warehouse=warehouse.as_uri(),
        **{
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        },
    )

    try:
        namespaces = catalog.list_namespaces()
    except Exception as exc:
        raise RuntimeError(
            f"Unable to read namespaces from catalog:\n{catalog_db}\n"
            f"Original error: {exc}"
        ) from exc

    normalized_namespaces = {
        ".".join(item) for item in namespaces
    }

    if namespace not in normalized_namespaces:
        raise RuntimeError(
            f"Namespace '{namespace}' was not found.\n"
            f"Available namespaces: {sorted(normalized_namespaces)}\n"
            "The dashboard will not create a replacement namespace."
        )

    return catalog


# ============================================================
# DISCOVER TABLES
# ============================================================

def list_iceberg_tables() -> list[str]:
    """List existing Iceberg table names in the configured namespace."""

    config = get_config()
    catalog = get_catalog()

    try:
        identifiers = catalog.list_tables(config["namespace"])
    except Exception as exc:
        raise RuntimeError(
            f"Unable to list tables in namespace "
            f"'{config['namespace']}': {exc}"
        ) from exc

    names: list[str] = []

    for identifier in identifiers:
        if isinstance(identifier, (tuple, list)):
            name = str(identifier[-1])
        else:
            name = str(identifier).split(".")[-1]

        names.append(name)

    return sorted(set(names))


# ============================================================
# LOAD ICEBERG TABLES
# ============================================================

def load_iceberg_tables() -> tuple[dict[str, Any], dict[str, str]]:
    """
    Read the existing Iceberg tables into Arrow tables.

    Returns:
        arrow_tables: {table_name: pyarrow.Table}
        schema_text:  {table_name: formatted column definitions}
    """
    config = get_config()
    catalog = get_catalog()
    namespace = config["namespace"]

    try:
        identifiers = catalog.list_tables(namespace)
    except Exception as exc:
        raise RuntimeError(
            f"Unable to list tables in namespace '{namespace}': {exc}"
        ) from exc

    if not identifiers:
        raise RuntimeError(
            f"No Iceberg tables were found in namespace '{namespace}'."
        )

    arrow_tables: dict[str, Any] = {}
    schema_text: dict[str, str] = {}

    for identifier in identifiers:
        if isinstance(identifier, (tuple, list)):
            table_name = str(identifier[-1])
            table_identifier = tuple(identifier)
        else:
            table_name = str(identifier).split(".")[-1]
            table_identifier = (namespace, table_name)

        full_name = f"{namespace}.{table_name}"

        try:
            # Use the exact identifier returned by the catalog.
            iceberg_table = catalog.load_table(table_identifier)

            iceberg_schema = iceberg_table.schema()

            schema_text[table_name] = ", ".join(
                f"{field.name} ({field.field_type})"
                for field in iceberg_schema.fields
            )

            # Read the current snapshot; do not write to Iceberg.
            arrow_table = iceberg_table.scan().to_arrow()

            arrow_tables[table_name] = arrow_table

            print(
                f"Loaded Iceberg table: {table_name} | "
                f"Rows: {arrow_table.num_rows:,} | "
                f"Columns: {arrow_table.num_columns}"
            )

        except Exception as exc:
            message = str(exc)

            if (
                "pyroaring" in message.lower()
                or "dll load failed" in message.lower()
            ):
                message += (
                    "\nPossible Windows native-extension compatibility "
                    "issue. Check the installed PyIceberg and pyroaring "
                    "versions in the active Python environment."
                )

            raise RuntimeError(
                f"Failed to read Iceberg table '{full_name}'.\n"
                f"Original error: {message}"
            ) from exc

    return arrow_tables, schema_text


# ============================================================
# DUCKDB REGISTRATION
# ============================================================

def make_duckdb_connection(
    arrow_tables: dict[str, Any],
) -> duckdb.DuckDBPyConnection:
    """
    Register loaded Arrow tables in an in-memory DuckDB connection.
    """
    connection = duckdb.connect(database=":memory:")

    try:
        connection.execute("SET enable_external_access = false")

        for table_name, arrow_table in arrow_tables.items():
            connection.register(table_name, arrow_table)

            escaped_name = table_name.replace('"', '""')

            connection.execute(
                f'SELECT * FROM "{escaped_name}" LIMIT 0'
            )

        return connection

    except Exception:
        connection.close()
        raise


# ============================================================
# TABLE PREVIEW
# ============================================================

def get_table_preview(
    arrow_tables: dict[str, Any],
    table_name: str,
    limit: int = 10,
):
    """Return a Pandas preview of an already-loaded Arrow table."""

    if table_name not in arrow_tables:
        raise KeyError(f"Unknown table: {table_name}")

    if limit < 1:
        raise ValueError("Limit must be greater than zero.")

    return arrow_tables[table_name].slice(0, limit).to_pandas()


# ============================================================
# LOCAL CONNECTION TEST
# ============================================================

if __name__ == "__main__":
    print("=" * 65)
    print("ICEBERG WAREHOUSE CONNECTION TEST")
    print("=" * 65)

    config = get_config()

    print("Catalog:   ", config["catalog_name"])
    print("Database:  ", config["catalog_db"])
    print("Warehouse: ", config["warehouse"])
    print("Namespace: ", config["namespace"])
    print()

    arrow_tables, schemas = load_iceberg_tables()
    connection = make_duckdb_connection(arrow_tables)

    try:
        print("\nRegistered table row counts:")

        for table_name in arrow_tables:
            escaped_name = table_name.replace('"', '""')

            count = connection.execute(
                f'SELECT COUNT(*) FROM "{escaped_name}"'
            ).fetchone()[0]

            print(f"  {table_name}: {count:,} rows")

        print("\nTable schemas:")

        for table_name, schema in schemas.items():
            print(f"\n{table_name}: {schema}")

        print("\nSUCCESS: Iceberg tables loaded into DuckDB.")

    finally:
        connection.close()
