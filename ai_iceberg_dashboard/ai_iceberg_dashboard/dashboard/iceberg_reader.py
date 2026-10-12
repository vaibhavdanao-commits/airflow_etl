from __future__ import annotations

import os
from pathlib import Path
from typing import Dict

import pandas as pd
from pyiceberg.catalog import load_catalog


def project_paths() -> tuple[Path, Path, Path, str]:
    """Resolve warehouse paths from .env, with the user's existing setup as defaults."""
    project_root = Path(os.getenv("PROJECT_ROOT", r"D:\airflow_etl")).expanduser()
    catalog_db = Path(os.getenv("CATALOG_DB", str(project_root / "Warehouse" / "dimensional_model_catalog.db"))).expanduser()
    warehouse_path = Path(os.getenv("WAREHOUSE_PATH", str(project_root / "Warehouse" / "dimensional_model"))).expanduser()
    namespace = os.getenv("NAMESPACE", "star_schema")
    return project_root, catalog_db, warehouse_path, namespace


def load_iceberg_tables() -> tuple[Dict[str, pd.DataFrame], dict]:
    """Read the existing Iceberg tables into pandas frames; does not create or mutate tables."""
    project_root, catalog_db, warehouse_path, namespace = project_paths()

    if not catalog_db.exists():
        raise FileNotFoundError(
            f"Iceberg catalog database was not found: {catalog_db}\n"
            "Check CATALOG_DB in dashboard/.env. This app will not create a replacement catalog."
        )
    if not warehouse_path.exists():
        raise FileNotFoundError(
            f"Iceberg warehouse folder was not found: {warehouse_path}\n"
            "Check WAREHOUSE_PATH in dashboard/.env."
        )

    catalog = load_catalog(
        "dimensional_model",
        type="sql",
        uri=f"sqlite:///{catalog_db.as_posix()}",
        warehouse=warehouse_path.resolve().as_uri(),
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"},
    )

    try:
        namespaces = catalog.list_namespaces()
    except Exception as exc:
        raise RuntimeError(f"Could not list Iceberg namespaces: {exc}") from exc

    normalized_namespaces = {".".join(x) for x in namespaces}
    if namespace not in normalized_namespaces:
        raise RuntimeError(
            f"Namespace '{namespace}' was not found in catalog '{catalog_db}'.\n"
            f"Namespaces detected: {sorted(normalized_namespaces) or 'none'}\n"
            "The dashboard intentionally will not create a namespace or another catalog."
        )

    identifiers = catalog.list_tables(namespace)
    if not identifiers:
        raise RuntimeError(f"No Iceberg tables found in namespace '{namespace}'.")

    frames: Dict[str, pd.DataFrame] = {}
    table_info: dict = {}
    for identifier in identifiers:
        # PyIceberg normally returns a tuple such as ('star_schema', 'fact_sales').
        table_name = identifier[-1] if isinstance(identifier, (tuple, list)) else str(identifier).split(".")[-1]
        full_identifier = f"{namespace}.{table_name}"
        try:
            table = catalog.load_table(full_identifier)
            arrow_table = table.scan().to_arrow()
            frame = arrow_table.to_pandas()
            frames[table_name] = frame
            table_info[table_name] = {
                "rows": len(frame),
                "columns": list(frame.columns),
                "schema": str(table.schema()),
            }
        except Exception as exc:
            message = str(exc)
            if "pyroaring" in message.lower() or "DLL load failed" in message:
                message += (
                    "\n\nThis looks like the Windows PyIceberg/pyroaring native DLL issue previously encountered. "
                    "The dashboard has not created a sample database or changed your warehouse. "
                    "Use a compatible Python 3.11/3.12 virtual environment and reinstall a compatible pyiceberg/pyroaring build."
                )
            raise RuntimeError(f"Failed reading Iceberg table '{full_identifier}': {message}") from exc

    return frames, {
        "project_root": str(project_root),
        "catalog_db": str(catalog_db),
        "warehouse_path": str(warehouse_path),
        "namespace": namespace,
        "tables": table_info,
    }
