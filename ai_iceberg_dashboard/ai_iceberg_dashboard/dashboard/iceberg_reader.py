
from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import duckdb
import pandas as pd
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog


# ============================================================
# CONFIGURATION
# ============================================================

HERE = Path(__file__).resolve().parent
load_dotenv(HERE / ".env")


def get_config() -> dict[str, str]:
    """Read configuration for the existing Iceberg warehouse."""
    return {
        "catalog_name": os.getenv(
            "ICEBERG_CATALOG_NAME", "dimensional_model"
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
            "ICEBERG_NAMESPACE", "star_schema"
        ),
    }


def project_paths() -> tuple[str, str, str, str]:
    """
    Return the four values expected by app.py:
    root, catalog database, warehouse path, namespace.
    """
    config = get_config()
    catalog_db = Path(config["catalog_db"]).expanduser().resolve()
    warehouse = Path(config["warehouse"]).expanduser().resolve()

    return (
        str(warehouse.parent),
        str(catalog_db),
        str(warehouse),
        config["namespace"],
    )


def windows_file_uri_to_path(value: str) -> str:
    """Convert a local Windows file URI to a native Windows path."""
    parsed = urlparse(value)

    if parsed.scheme.lower() != "file":
        return value

    path = unquote(parsed.path)

    # file:///D:/folder/file -> D:/folder/file
    if len(path) >= 3 and path.startswith("/") and path[2] == ":":
        path = path[1:]

    if parsed.netloc and parsed.netloc.lower() != "localhost":
        path = f"//{parsed.netloc}{path}"

    return str(Path(path))


# ============================================================
# EXISTING ICEBERG CATALOG
# ============================================================

def get_catalog():
    """
    Connect to the existing SQL-backed Iceberg catalog.
    Does not intentionally create a replacement catalog or tables.
    """
    config = get_config()

    catalog_db = Path(config["catalog_db"]).expanduser().resolve()
    warehouse = Path(config["warehouse"]).expanduser().resolve()
    namespace = config["namespace"]

    if not catalog_db.is_file():
        raise FileNotFoundError(
            f"Catalog database not found:\n{catalog_db}\n"
            "Check ICEBERG_CATALOG_DB in .env."
        )

    if not warehouse.is_dir():
        raise FileNotFoundError(
            f"Warehouse directory not found:\n{warehouse}\n"
            "Check ICEBERG_WAREHOUSE in .env."
        )

    catalog = load_catalog(
        config["catalog_name"],
        type="sql",
        uri=f"sqlite:///{catalog_db.as_posix()}",
        warehouse=warehouse.as_uri(),
        **{
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        },
    )

    namespaces = catalog.list_namespaces()
    available = {".".join(ns) for ns in namespaces}

    if namespace not in available:
        raise RuntimeError(
            f"Namespace '{namespace}' was not found.\n"
            f"Available namespaces: {sorted(available)}"
        )

    return catalog


def list_iceberg_tables() -> list[str]:
    """Return table names from the configured namespace."""
    config = get_config()
    catalog = get_catalog()

    identifiers = catalog.list_tables(config["namespace"])
    names = []

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

def load_iceberg_tables() -> tuple[
    dict[str, pd.DataFrame], dict[str, Any]
]:
    """
    Load existing Iceberg tables into Pandas DataFrames.

    Returns:
        frames:
            {table_name: Pandas DataFrame}

        info:
            {
                "namespace": ...,
                "tables": {
                    table_name: {
                        "rows": ...,
                        "columns": [...]
                    }
                },
                "schema_text": {
                    table_name: "column (type), ..."
                }
            }

    This function reads data only. It does not write to Iceberg.
    """
    config = get_config()
    catalog = get_catalog()
    namespace = config["namespace"]

    identifiers = catalog.list_tables(namespace)

    if not identifiers:
        raise RuntimeError(
            f"No Iceberg tables found in namespace '{namespace}'."
        )

    frames: dict[str, pd.DataFrame] = {}
    table_info: dict[str, dict[str, Any]] = {}
    schema_text: dict[str, str] = {}

    for identifier in identifiers:
        if isinstance(identifier, (tuple, list)):
            table_identifier = tuple(identifier)
            table_name = str(identifier[-1])
        else:
            table_name = str(identifier).split(".")[-1]
            table_identifier = (namespace, table_name)

        full_name = f"{namespace}.{table_name}"

        try:
            iceberg_table = catalog.load_table(table_identifier)
            iceberg_schema = iceberg_table.schema()

            schema_text[table_name] = ", ".join(
                f"{field.name} ({field.field_type})"
                for field in iceberg_schema.fields
            )

            # Read the existing table snapshot; never modify Iceberg.
            arrow_table = iceberg_table.scan().to_arrow()
            frame = arrow_table.to_pandas()

            frames[table_name] = frame
            table_info[table_name] = {
                "rows": len(frame),
                "columns": list(frame.columns),
            }

            print(
                f"Loaded Iceberg table: {table_name} | "
                f"Rows: {len(frame):,} | "
                f"Columns: {len(frame.columns)}"
            )

        except Exception as exc:
            raise RuntimeError(
                f"Failed to read Iceberg table '{full_name}': {exc}"
            ) from exc

    info: dict[str, Any] = {
        "namespace": namespace,
        "tables": table_info,
        "schema_text": schema_text,
    }

    return frames, info


# ============================================================
# DUCKDB
# ============================================================

def make_duckdb_connection(
    frames: dict[str, Any],
) -> duckdb.DuckDBPyConnection:
    """Register DataFrames in a new in-memory DuckDB connection."""
    connection = duckdb.connect(database=":memory:")

    try:
        connection.execute("SET enable_external_access = false")

        for table_name, frame in frames.items():
            # Quote identifiers safely.
            escaped_name = table_name.replace('"', '""')
            connection.register(table_name, frame)

            connection.execute(
                f'SELECT * FROM "{escaped_name}" LIMIT 0'
            )

        return connection

    except Exception:
        connection.close()
        raise


def get_table_preview(
    frames: dict[str, pd.DataFrame],
    table_name: str,
    limit: int = 10,
) -> pd.DataFrame:
    """Return a preview of an already-loaded table."""
    if table_name not in frames:
        raise KeyError(f"Unknown table: {table_name}")

    if limit < 1:
        raise ValueError("limit must be greater than zero")

    return frames[table_name].head(limit)


# ============================================================
# LOCAL TEST
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

    frames, info = load_iceberg_tables()
    connection = make_duckdb_connection(frames)

    try:
        print("\nRegistered table row counts:")

        for table_name in frames:
            escaped_name = table_name.replace('"', '""')
            count = connection.execute(
                f'SELECT COUNT(*) FROM "{escaped_name}"'
            ).fetchone()[0]

            print(f"  {table_name}: {count:,} rows")

        print("\nSUCCESS: Iceberg tables loaded into DuckDB.")
        print("Info keys:", list(info.keys()))
        print("Tables:", list(info["tables"].keys()))

    finally:
        connection.close()
