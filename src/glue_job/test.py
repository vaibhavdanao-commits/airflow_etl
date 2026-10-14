
from pathlib import Path
from functools import lru_cache

import streamlit as st
from pyiceberg.catalog import load_catalog
from langchain_ollama import ChatOllama

# ============================================================
# PROJECT CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CURATED_FOLDER = PROJECT_ROOT / "curated"
WAREHOUSE_FOLDER = PROJECT_ROOT / "Warehouse"

CATALOG_DB = WAREHOUSE_FOLDER / "pyiceberg_catalog.db"
WAREHOUSE_URI = WAREHOUSE_FOLDER.resolve().as_uri()

NAMESPACE = "default"
CATALOG_NAME = "local"

# Local Ollama configuration
OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_MODEL = "qwen2.5-coder:7b"


# ============================================================
# LOAD EXISTING CATALOG
# ============================================================

@st.cache_resource
def get_catalog():
    if not CATALOG_DB.exists():
        raise FileNotFoundError(
            f"Catalog database does not exist: {CATALOG_DB}"
        )

    return load_catalog(
        CATALOG_NAME,
        type="sql",
        uri=f"sqlite:///{CATALOG_DB.as_posix()}",
        warehouse=WAREHOUSE_URI,
        **{
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
        },
    )


# ============================================================
# DISCOVER AND LOAD ALL REGISTERED TABLES
# ============================================================

@st.cache_resource
def initialize_warehouse():
    catalog = get_catalog()

    namespaces = catalog.list_namespaces()

    if (NAMESPACE,) not in namespaces:
        raise RuntimeError(
            f"Namespace '{NAMESPACE}' not found. "
            f"Available namespaces: {namespaces}"
        )

    identifiers = catalog.list_tables(NAMESPACE)

    if not identifiers:
        raise RuntimeError(
            f"No Iceberg tables are registered in namespace '{NAMESPACE}'."
        )

    tables = {}
    errors = {}

    for identifier in identifiers:
        table_name = identifier[-1]

        try:
            iceberg_table = catalog.load_table(identifier)

            # Scan to validate metadata and read the actual table.
            # This requires the PyIceberg dependencies to work.
            arrow_table = iceberg_table.scan().to_arrow()

            tables[table_name] = {
                "iceberg_table": iceberg_table,
                "arrow_table": arrow_table,
                "row_count": arrow_table.num_rows,
                "columns": arrow_table.column_names,
            }

        except Exception as exc:
            errors[table_name] = str(exc)

    return tables, errors


# ============================================================
# LOCAL LANGUAGE MODEL
# ============================================================

@st.cache_resource
def get_local_llm():
    return ChatOllama(
        model=OLLAMA_MODEL,
        base_url=OLLAMA_BASE_URL,
        temperature=0,
    )


# ============================================================
# STREAMLIT INITIALIZATION
# ============================================================

def show_warehouse_status():
    st.title("🤖 AI-Driven Dimensional Warehouse Dashboard")
    st.caption(
        "Ask questions in natural language and analyze your existing "
        "Iceberg warehouse using local Ollama."
    )

    st.write(f"**Project:** `{PROJECT_ROOT}`")
    st.write(f"**Catalog:** `{CATALOG_DB}`")
    st.write(f"**Namespace:** `{NAMESPACE}`")

    try:
        tables, errors = initialize_warehouse()

        if tables:
            st.success(f"Loaded {len(tables)} Iceberg table(s).")

            for name, details in tables.items():
                st.write(
                    f"**{name}** — {details['row_count']:,} rows"
                )
                st.caption(", ".join(details["columns"]))

        if errors:
            st.warning(
                f"{len(errors)} table(s) could not be read."
            )

            for name, error in errors.items():
                st.error(f"{name}: {error}")

        if not tables:
            st.error(
                "The catalog contains registered tables, but none "
                "could be read. Resolve the table-loading errors above."
            )
            st.stop()

        # Test local Ollama only after the warehouse loads.
        st.subheader("Ask your warehouse")

        question = st.text_input(
            "Enter a question about your data",
            placeholder="Which products generate the most revenue?",
        )

        if st.button("Ask AI", type="primary", disabled=not question.strip()):
            with st.spinner("Analyzing your question..."):
                try:
                    llm = get_local_llm()

                    table_summary = "\n".join(
                        f"{name}: {', '.join(details['columns'])}"
                        for name, details in tables.items()
                    )

                    prompt = f"""
You are a data warehouse analytics assistant.

Available Iceberg tables and columns:
{table_summary}

User question:
{question}

Explain which tables and columns are relevant.
Do not invent columns or claim you executed SQL.
If SQL is needed, provide a query using only the listed tables
and columns, and clearly label it as proposed SQL.
"""

                    response = llm.invoke(prompt)
                    st.markdown(response.content)

                except Exception as exc:
                    st.error(
                        f"Ollama request failed: {exc}"
                    )

    except Exception as exc:
        st.error(f"Warehouse initialization failed: {exc}")
        st.info(
            "Check the printed table-loading errors, catalog path, "
            "and PyIceberg native dependencies."
        )


if __name__ == "__main__":
    show_warehouse_status()