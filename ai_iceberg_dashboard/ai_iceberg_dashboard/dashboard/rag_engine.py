
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from langchain_chroma import Chroma
from langchain_core.documents import Document
from langchain_ollama import OllamaEmbeddings

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------

HERE = Path(__file__).resolve().parent
load_dotenv(HERE / ".env")


def get_embeddings() -> OllamaEmbeddings:
    """Create the local Ollama embedding client."""

    return OllamaEmbeddings(
        model=os.getenv(
            "OLLAMA_EMBED_MODEL",
            "nomic-embed-text",
        ),
        base_url=os.getenv(
            "OLLAMA_BASE_URL",
            "http://localhost:11434",
        ),
    )


# ------------------------------------------------------------
# BUILD KNOWLEDGE DOCUMENTS
# ------------------------------------------------------------

def build_knowledge_documents(
    schema_text: dict[str, str],
) -> list[Document]:
    """
    Create RAG documents from live Iceberg schema definitions
    and retail analytics guidance.
    """

    documents: list[Document] = []

    # Add one document for each actual warehouse table.
    for table_name, columns in schema_text.items():
        documents.append(
            Document(
                page_content=(
                    f"Warehouse table: {table_name}\n"
                    f"Columns and data types: {columns}\n"
                    "Use only these actual columns when generating SQL. "
                    "Identify relevant fact and dimension tables and join "
                    "them only through matching keys present in the schema."
                ),
                metadata={
                    "kind": "schema",
                    "table": table_name,
                },
            )
        )

    # Business guidance improves retrieval and SQL generation.
    business_rules = [
        {
            "topic": "Monthly sales",
            "content": (
                "For monthly sales analysis, identify the actual date column "
                "or date dimension in the live schema. Group by month and "
                "sort chronologically. Do not invent date columns."
            ),
        },
        {
            "topic": "Revenue analysis",
            "content": (
                "For revenue analysis, inspect the available fact-table "
                "measures. Use a documented revenue or sales amount column "
                "when present. Otherwise derive revenue only when the "
                "available columns and business definition support it. "
                "Do not assume that quantity multiplied by unit price is "
                "always the official revenue definition."
            ),
        },
        {
            "topic": "Product analysis",
            "content": (
                "For product questions, identify the actual product key and "
                "product description in the schema. Filter the requested "
                "product and aggregate the relevant measure."
            ),
        },
        {
            "topic": "Customer city analysis",
            "content": (
                "For city-based questions, identify the actual city column "
                "in a relevant customer or store dimension. Join through "
                "valid key relationships before filtering."
            ),
        },
        {
            "topic": "Store analysis",
            "content": (
                "For store comparisons, use the actual store dimension and "
                "fact-table store key. Group by an available store label."
            ),
        },
        {
            "topic": "Read-only SQL",
            "content": (
                "Generate one DuckDB-compatible SELECT statement. Never "
                "generate INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, COPY, "
                "external file readers, network access or multiple statements."
            ),
        },
        {
            "topic": "Schema accuracy",
            "content": (
                "The live Iceberg schema is authoritative. Retrieved examples "
                "are guidance only. If a requested metric cannot be calculated "
                "from the actual columns, explain the limitation."
            ),
        },
    ]

    for rule in business_rules:
        documents.append(
            Document(
                page_content=(
                    f"Business rule: {rule['topic']}\n"
                    f"{rule['content']}"
                ),
                metadata={
                    "kind": "business_rule",
                    "topic": rule["topic"],
                },
            )
        )

    # SQL patterns are intentionally described as patterns, not hard-coded
    # queries against columns that may not exist in the user's schema.
    sql_patterns = [
        (
            "Monthly revenue by product",
            "Identify the actual date field, product field and valid measure. "
            "Filter the requested product, group by month and order by month.",
        ),
        (
            "Top products",
            "Join the fact table to the product dimension through a real key, "
            "aggregate a supported measure, order descending and limit results.",
        ),
        (
            "Sales by city",
            "Identify the actual city column, join the appropriate dimension "
            "to the fact table through a valid key, filter or group by city, "
            "and aggregate a supported measure.",
        ),
        (
            "Sales by store",
            "Use the actual store dimension and store key. Group by the store "
            "label and aggregate a supported sales measure.",
        ),
    ]

    for title, pattern in sql_patterns:
        documents.append(
            Document(
                page_content=(
                    f"SQL example pattern: {title}\n{pattern}"
                ),
                metadata={
                    "kind": "sql_pattern",
                    "topic": title,
                },
            )
        )

    return documents


# ------------------------------------------------------------
# CREATE OR OPEN CHROMADB VECTOR STORE
# ------------------------------------------------------------

def get_vectorstore(
    schema_text: dict[str, str] | None = None,
) -> Chroma:
    """
    Open the persistent ChromaDB collection.

    The optional schema_text argument is retained for easy integration
    with the dashboard. Call rebuild_index() to index current knowledge.
    """

    chroma_path = Path(
        os.getenv("CHROMA_PATH", ".\\chroma_db")
    )

    if not chroma_path.is_absolute():
        chroma_path = HERE / chroma_path

    chroma_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    return Chroma(
        collection_name=os.getenv(
            "CHROMA_COLLECTION",
            "retail_warehouse_knowledge",
        ),
        embedding_function=get_embeddings(),
        persist_directory=str(chroma_path.resolve()),
    )


# ------------------------------------------------------------
# BUILD OR REFRESH RAG INDEX
# ------------------------------------------------------------

def rebuild_index(
    schema_text: dict[str, str],
) -> tuple[Chroma, int]:
    """
    Rebuild only the ChromaDB knowledge collection.

    This does not create, update, append to, or delete Iceberg tables.
    """

    documents = build_knowledge_documents(schema_text)

    if not documents:
        raise ValueError(
            "No RAG documents were generated. Check the Iceberg schema."
        )

    store = get_vectorstore(schema_text)

    # Clear only the configured ChromaDB collection.
    # The Iceberg catalog and warehouse are not touched.
    try:
        store.delete_collection()
    except Exception:
        pass

    store = get_vectorstore(schema_text)

    # Embed and persist knowledge documents locally.
    store.add_documents(documents)

    return store, len(documents)


# ------------------------------------------------------------
# RETRIEVE CONTEXT FOR A USER QUESTION
# ------------------------------------------------------------

def retrieve_context(
    store: Chroma,
    question: str,
    k: int = 6,
) -> str:
    """
    Retrieve the most relevant knowledge documents for a question.
    """

    question = question.strip()

    if not question:
        return "No question supplied."

    if k < 1:
        k = 1

    documents = store.similarity_search(
        query=question,
        k=k,
    )

    if not documents:
        return (
            "No matching RAG documents were found. "
            "Use the live warehouse schema as the authoritative source."
        )

    sections = []

    for index, document in enumerate(documents, start=1):
        kind = document.metadata.get(
            "kind",
            "knowledge",
        )

        table = document.metadata.get(
            "table",
            "",
        )

        header = f"Retrieved document {index} [{kind}]"

        if table:
            header += f" [table={table}]"

        sections.append(
            f"{header}\n{document.page_content}"
        )

    return "\n\n---\n\n".join(sections)


# ------------------------------------------------------------
# TEST RAG ENGINE
# ------------------------------------------------------------

if __name__ == "__main__":
    from iceberg_reader import load_iceberg_tables

    print("=" * 60)
    print("LOCAL RAG ENGINE TEST")
    print("=" * 60)

    # Read current Iceberg schema; no warehouse writes occur.
    _, schemas = load_iceberg_tables()

    print("\nCreating ChromaDB index...")

    vectorstore, document_count = rebuild_index(schemas)

    print(
        f"Indexed {document_count} knowledge documents."
    )

    test_question = (
        "How can I calculate monthly revenue by product?"
    )

    print(f"\nTest question: {test_question}")

    context = retrieve_context(
        vectorstore,
        test_question,
        k=5,
    )

    print("\nRetrieved context:\n")
    print(context)

    print("\nSUCCESS: RAG indexing and retrieval completed.")
