
from __future__ import annotations

import json
import os
import re
from typing import Any
from urllib.parse import urlparse

import duckdb
import pandas as pd
import sqlglot
from sqlglot import exp
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

MAX_RESULT_ROWS = max(
    1, int(os.getenv("MAX_RESULT_ROWS", "500"))
)

MAX_SQL_RETRIES = max(
    0, int(os.getenv("MAX_SQL_RETRIES", "2"))
)

LLM_TIMEOUT_SECONDS = max(
    10, int(os.getenv("LLM_TIMEOUT_SECONDS", "120"))
)

LLM_MAX_TOKENS = max(
    256, int(os.getenv("LLM_MAX_TOKENS", "800"))
)

LLM_TEMPERATURE = float(
    os.getenv("LLM_TEMPERATURE", "0.1")
)

ALLOWED_CHARTS = {
    "auto", "bar", "line", "pie", "donut", "area",
    "scatter", "histogram", "box", "violin", "treemap",
    "sunburst", "heatmap", "funnel", "table", "kpi",
}

BLOCKED_FUNCTIONS = {
    "READ_CSV",
    "READ_CSV_AUTO",
    "READ_PARQUET",
    "READ_JSON",
    "READ_JSON_AUTO",
    "PARQUET_SCAN",
    "CSV_SCAN",
    "JSON_SCAN",
    "GLOB",
    "HTTPFS",
    "READ_TEXT",
    "READ_BLOB",
    "READ_NPY",
    "READ_XLS",
    "READ_XLSX",
    "QUERY",
    "QUERY_TABLE",
    "EXECUTE",
    "INSTALL",
    "LOAD",
    "FORCE_CHECKPOINT",
}

FORBIDDEN_EXPRESSIONS = (
    "Insert",
    "Update",
    "Delete",
    "Drop",
    "Create",
    "Command",
    "Copy",
    "Pragma",
    "Attach",
    "Detach",
    "Transaction",
    "Merge",
    "Into",
    "Grant",
    "Revoke",
    "Use",
    "Set",
    "Analyze",
    "TruncateTable",
)


# ---------------------------------------------------------------------
# Ollama connection
# ---------------------------------------------------------------------

def make_llm() -> ChatOpenAI:
    """
    Connect to local Ollama using its OpenAI-compatible endpoint.

    The endpoint may be configured as either:
        http://localhost:11434
        http://localhost:11434/v1
    """

    base_url = os.getenv(
        "OLLAMA_BASE_URL",
        "http://localhost:11434/v1",
    ).strip().rstrip("/")

    model = os.getenv(
        "OLLAMA_MODEL",
        "llama3.2",
    ).strip()

    if not model:
        raise ValueError(
            "OLLAMA_MODEL is empty. Check your .env file."
        )

    parsed = urlparse(base_url)

    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(
            "Invalid OLLAMA_BASE_URL. Use "
            "http://localhost:11434/v1."
        )

    if parsed.path in ("", "/"):
        base_url += "/v1"

    return ChatOpenAI(
        model=model,
        base_url=base_url,
        api_key=os.getenv("OLLAMA_API_KEY", "ollama"),
        temperature=LLM_TEMPERATURE,
        max_tokens=LLM_MAX_TOKENS,
        max_retries=0,
        timeout=LLM_TIMEOUT_SECONDS,
    )


def invoke_llm(
    system_prompt: str,
    user_prompt: str,
) -> str:
    """Invoke Ollama and return its response text."""

    try:
        response = make_llm().invoke(
            [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_prompt),
            ]
        )

        text = response_text(response)

        if not text.strip():
            raise ValueError(
                "Ollama returned an empty response."
            )

        return text

    except Exception as exc:
        message = str(exc)

        if (
            "token repeat limit reached" in message.lower()
            or "prediction aborted" in message.lower()
        ):
            raise RuntimeError(
                "Ollama aborted generation because of repeated "
                "tokens. Try a shorter prompt, verify the configured "
                "model with 'ollama list', and consider testing "
                "another locally installed model. The dashboard "
                "cannot safely use an incomplete AI response."
            ) from exc

        if "connection" in message.lower():
            raise RuntimeError(
                "Cannot connect to Ollama. Start Ollama and verify "
                "OLLAMA_BASE_URL and OLLAMA_MODEL in your .env file."
            ) from exc

        raise RuntimeError(
            f"Ollama request failed: {message}"
        ) from exc


def response_text(response: Any) -> str:
    """Convert a LangChain response into plain text."""

    content = getattr(response, "content", response)

    if isinstance(content, str):
        return content

    if isinstance(content, list):
        parts: list[str] = []

        for item in content:
            if isinstance(item, dict):
                parts.append(str(item.get("text", "")))
            else:
                parts.append(str(item))

        return "".join(parts)

    return str(content)


# ---------------------------------------------------------------------
# JSON parsing
# ---------------------------------------------------------------------

def extract_json(text: str) -> dict[str, Any]:
    """Extract a JSON object, with a fallback for SQL-only responses."""

    if not text or not text.strip():
        raise ValueError(
            "Ollama returned an empty response. Check the model."
        )

    cleaned = text.strip()

    cleaned = re.sub(
        r"^\s*```(?:json)?\s*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )

    cleaned = re.sub(
        r"\s*```\s*$",
        "",
        cleaned,
    )

    try:
        result = json.loads(cleaned)

        if isinstance(result, dict):
            return result

    except json.JSONDecodeError:
        pass

    decoder = json.JSONDecoder()

    for match in re.finditer(r"\{", cleaned):
        try:
            result, _ = decoder.raw_decode(
                cleaned[match.start():]
            )

            if isinstance(result, dict) and "sql" in result:
                return result

        except json.JSONDecodeError:
            continue

    sql_match = re.search(
        r"```(?:sql)?\s*(.*?)```",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )

    if sql_match:
        candidate = sql_match.group(1).strip()

        if re.match(
            r"^(SELECT|WITH)\b",
            candidate,
            flags=re.IGNORECASE,
        ):
            return {
                "sql": candidate,
                "chart_type": "auto",
                "title": "Analysis Results",
                "explanation": (
                    "SQL extracted from the model response."
                ),
                "metric": "",
                "dimension": "",
            }

    raise ValueError(
        "Ollama did not return valid JSON or recognizable SQL. "
        "Try a shorter question or check the configured model. "
        f"Response received: {cleaned[:500]}"
    )


# ---------------------------------------------------------------------
# Actual warehouse schema
# ---------------------------------------------------------------------

def schema_description(
    frames: dict[str, pd.DataFrame],
) -> str:
    """
    Describe actual registered tables and their columns.

    Keep schema descriptions compact to reduce local model repetition.
    """

    chunks: list[str] = []

    for name, df in frames.items():
        chunks.append(
            f'\nTABLE "{name}" ({len(df):,} rows)'
        )

        for column in df.columns:
            series = df[column]

            details = (
                f'  "{column}" ({series.dtype})'
            )

            if (
                pd.api.types.is_object_dtype(series)
                or isinstance(
                    series.dtype,
                    pd.CategoricalDtype,
                )
            ):
                examples = (
                    series.dropna()
                    .astype(str)
                    .drop_duplicates()
                    .head(4)
                    .tolist()
                )

                if examples:
                    details += (
                        "; examples="
                        + json.dumps(
                            examples,
                            ensure_ascii=False,
                            default=str,
                        )
                    )

            chunks.append(details)

    return "\n".join(chunks)


def live_schema_description(
    connection: duckdb.DuckDBPyConnection,
    frames: dict[str, pd.DataFrame],
) -> str:
    """
    Get live column metadata from DuckDB.

    DataFrame metadata is a fallback when DESCRIBE is unavailable.
    """

    chunks: list[str] = []

    for table_name in frames:
        # Names come from the registered application table list,
        # not from the user's natural-language question.
        escaped_name = table_name.replace('"', '""')

        try:
            schema_df = connection.execute(
                f'DESCRIBE "{escaped_name}"'
            ).fetchdf()

            columns = []

            for _, row in schema_df.iterrows():
                columns.append(
                    f'- "{row["column_name"]}" '
                    f'({row["column_type"]})'
                )

            chunks.append(
                f'TABLE "{table_name}"\n'
                + "\n".join(columns)
            )

        except Exception:
            fallback = frames[table_name]

            columns = [
                f'- "{column}" ({dtype})'
                for column, dtype in zip(
                    fallback.columns,
                    fallback.dtypes,
                )
            ]

            chunks.append(
                f'TABLE "{table_name}"\n'
                + "\n".join(columns)
            )

    return "\n\n".join(chunks)


# ---------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------

def build_system_prompt(
    schema: str,
    question: str,
) -> str:
    """Build a compact, schema-aware retail analytics prompt."""

    return f"""
You are a careful retail analytics engineer writing DuckDB SQL.

Return exactly one JSON object with these keys:
sql, chart_type, title, explanation, metric, dimension.

ACTUAL REGISTERED TABLES AND COLUMNS:
{schema}

SCHEMA RULES:
- Use only tables and columns listed above.
- Never invent a column, table, key, metric, or join relationship.
- A column in a dimension table cannot be referenced as if it were
  in fact_sales. Join the tables using verified matching keys.
- Before using payment_method, verify that it exists in an available
  table. If it does not exist, return sql="" and explain that the
  warehouse needs this attribute.
- For product analysis, inspect dim_product and fact_sales.
- For customer/city analysis, inspect dim_customer and fact_sales.
- For time analysis, inspect dim_date and fact_sales.
- Use only matching join keys shown in the schema.
- Avoid joins that multiply fact rows.
- If a necessary column or relationship is missing, return sql=""
  and explain what is missing.
- Do not return a query that you cannot support from the schema.

METRIC RULES:
- Revenue: use an actual revenue/sales amount column when available.
- Otherwise calculate quantity * price only if both columns exist
  in a valid joined table and their meaning supports the calculation.
- Units sold: SUM of the actual quantity column.
- Order count: COUNT(DISTINCT order_id) only if order_id exists.
- Average order value: supported revenue divided by distinct orders.
- Do not assume sales_amount is the same as payment amount.
- Do not fabricate revenue, order counts, or customer attributes.

QUERY RULES:
- Generate one read-only SELECT query; CTEs are allowed.
- Never use SELECT * for analytical aggregation.
- Preserve the requested filters, product identifiers, city, and dates.
- Do not add arbitrary date filters.
- For monthly trends, return one row per month, ordered chronologically.
- Use clear output aliases such as month, product_name, total_sales.
- Do not pivot months into separate columns unless explicitly requested.
- Use DuckDB SQL syntax.
- Do not access files, URLs, external databases, or external tables.
- Do not use INSERT, UPDATE, DELETE, CREATE, DROP, COPY, ATTACH,
  PRAGMA, or other write/administrative statements.

QUESTION:
{question}

CHART TYPE:
Choose one: auto, bar, line, pie, donut, area, scatter, histogram,
box, violin, treemap, sunburst, heatmap, funnel, table, kpi.

Use line for time trends, bar for comparisons, donut/pie for a small
number of categories, heatmap for two dimensions, table for detail,
and kpi for a single value.

If the request is unsupported, return:
{{"sql":"","chart_type":"table","title":"Unavailable data",
"explanation":"Explain the exact missing column or relationship.",
"metric":"","dimension":""}}

Return valid JSON only. No Markdown fences. Keep the response concise.
""".strip()


# ---------------------------------------------------------------------
# Analysis result normalization
# ---------------------------------------------------------------------

def normalize_analysis(
    result: dict[str, Any],
) -> dict[str, Any]:
    """Ensure the model result has the expected fields."""

    normalized = {
        "sql": result.get("sql", ""),
        "chart_type": result.get("chart_type", "auto"),
        "title": result.get("title", "Analysis Results"),
        "explanation": result.get("explanation", ""),
        "metric": result.get("metric", ""),
        "dimension": result.get("dimension", ""),
    }

    normalized["sql"] = str(
        normalized["sql"] or ""
    ).strip().rstrip(";")

    normalized["chart_type"] = str(
        normalized["chart_type"] or "auto"
    ).strip().lower()

    if normalized["chart_type"] not in ALLOWED_CHARTS:
        normalized["chart_type"] = "auto"

    for key in (
        "title",
        "explanation",
        "metric",
        "dimension",
    ):
        normalized[key] = str(
            normalized[key] or ""
        ).strip()

    return normalized


# ---------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------

def generate_analysis(
    question: str,
    frames: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    """Generate schema-aware SQL from a business question."""

    if not question or not question.strip():
        raise ValueError("Please enter a business question.")

    if not frames:
        raise ValueError("No warehouse tables were loaded.")

    schema = schema_description(frames)

    system_prompt = build_system_prompt(
        schema=schema,
        question=question.strip(),
    )

    raw_response = invoke_llm(
        system_prompt=system_prompt,
        user_prompt=(
            "Generate the SQL analysis for this business question. "
            "Use only the schema supplied in the system message.\n"
            f"Question: {question.strip()}"
        ),
    )

    return normalize_analysis(
        extract_json(raw_response)
    )


# ---------------------------------------------------------------------
# SQL validation
# ---------------------------------------------------------------------

def validate_sql(
    sql: str,
    available_tables: list[str],
) -> str:
    """
    Allow a single read-only SELECT against registered tables.

    DuckDB performs the final column and type binding checks.
    """

    if not sql or not sql.strip():
        raise ValueError(
            "The AI did not generate SQL. The requested data may "
            "be unavailable in the warehouse."
        )

    sql = sql.strip().rstrip(";")

    blocked_pattern = (
        r"\b(?:"
        + "|".join(
            re.escape(name)
            for name in BLOCKED_FUNCTIONS
        )
        + r")\s*\("
    )

    if re.search(
        blocked_pattern,
        sql,
        flags=re.IGNORECASE,
    ):
        raise ValueError(
            "External or file-reading SQL functions are prohibited."
        )

    try:
        statements = sqlglot.parse(
            sql,
            read="duckdb",
        )
    except Exception as exc:
        raise ValueError(
            f"Generated SQL could not be parsed: {exc}"
        ) from exc

    if len(statements) != 1 or statements[0] is None:
        raise ValueError("Only one SQL statement is allowed.")

    tree = statements[0]

    if not isinstance(tree, (exp.Select, exp.Union)):
        raise ValueError(
            "Only read-only SELECT queries are allowed."
        )

    for name in FORBIDDEN_EXPRESSIONS:
        node_type = getattr(exp, name, None)

        if node_type is not None and tree.find(node_type):
            raise ValueError(
                "The generated SQL contains a prohibited operation."
            )

    for node in tree.walk():
        if isinstance(node, exp.Func):
            function_name = node.sql_name().upper()

            if function_name in BLOCKED_FUNCTIONS:
                raise ValueError(
                    f"Function '{function_name}' is prohibited."
                )

    allowed_tables = {
        table.lower()
        for table in available_tables
    }

    cte_names = {
        cte.alias_or_name.lower()
        for cte in tree.find_all(exp.CTE)
    }

    for table in tree.find_all(exp.Table):
        table_name = table.name.lower()

        if table.db or table.catalog:
            raise ValueError(
                "Qualified or external table references are prohibited. "
                "Use registered warehouse table names only."
            )

        if (
            table_name not in allowed_tables
            and table_name not in cte_names
        ):
            raise ValueError(
                f"Unknown table '{table.name}'. "
                f"Available tables: {', '.join(available_tables)}"
            )

    return tree.sql(dialect="duckdb")


# ---------------------------------------------------------------------
# Read-only SQL execution
# ---------------------------------------------------------------------

def run_read_only_query(
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    available_tables: list[str],
) -> pd.DataFrame:
    """Validate and execute a query with a bounded result."""

    safe_sql = validate_sql(
        sql,
        available_tables,
    )

    # A transaction and a read-only connection are preferable when
    # configuring DuckDB. This function additionally restricts SQL.
    bounded_sql = (
        f"SELECT * FROM ({safe_sql}) AS dashboard_result "
        f"LIMIT {MAX_RESULT_ROWS}"
    )

    try:
        return connection.execute(
            bounded_sql
        ).fetchdf()

    except Exception as exc:
        raise RuntimeError(
            f"SQL execution failed: {exc}\n\n"
            f"Generated SQL:\n{safe_sql}"
        ) from exc


# ---------------------------------------------------------------------
# SQL repair
# ---------------------------------------------------------------------

def repair_sql(
    question: str,
    schema: str,
    previous_sql: str,
    error: str,
) -> dict[str, Any]:
    """Ask Ollama to repair a failed query using actual schema."""

    system_prompt = f"""
You repair DuckDB SELECT queries for a retail analytics dashboard.

ACTUAL TABLES AND COLUMNS:
{schema}

Rules:
- Use only listed tables and columns.
- Fix the specific error shown by DuckDB.
- Preserve the user's business intent and filters.
- Never invent a missing column or join key.
- Never use payment_method unless it exists in the schema.
- Use valid DuckDB SQL.
- Generate one read-only SELECT query or return sql="" if impossible.
- Keep the output short to avoid repetitive generation.
- Return JSON only with keys: sql, chart_type, title,
  explanation, metric, dimension.
""".strip()

    user_prompt = f"""
Business question: {question}

Previous SQL:
{previous_sql}

DuckDB error:
{error}

Return the corrected query as JSON. If the schema cannot support
the question, return an empty SQL string and explain why.
""".strip()

    return normalize_analysis(
        extract_json(
            invoke_llm(
                system_prompt,
                user_prompt,
            )
        )
    )


# ---------------------------------------------------------------------
# Generate, execute, and automatically repair SQL
# ---------------------------------------------------------------------

def generate_and_run(
    question: str,
    frames: dict[str, pd.DataFrame],
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, Any]:
    """
    Generate SQL, execute it, and repair failed queries.

    MAX_SQL_RETRIES=2 permits at most three query execution attempts.
    """

    if not question or not question.strip():
        raise ValueError("Please enter a business question.")

    if not frames:
        raise ValueError("No warehouse tables were loaded.")

    available_tables = list(frames.keys())

    schema = live_schema_description(
        connection,
        frames,
    )

    analysis = generate_analysis(
        question,
        frames,
    )

    sql = analysis.get("sql", "").strip()

    if not sql:
        return {
            **analysis,
            "data": pd.DataFrame(),
            "success": False,
            "attempts": 0,
            "error": (
                analysis.get("explanation")
                or "The warehouse schema does not support this question."
            ),
        }

    last_error = ""

    for attempt in range(MAX_SQL_RETRIES + 1):
        try:
            result_df = run_read_only_query(
                connection=connection,
                sql=sql,
                available_tables=available_tables,
            )

            explanation = str(
                analysis.get("explanation", "")
            ).strip()

            if result_df.empty:
                explanation = (
                    f"{explanation} The query returned no rows. "
                    "Check the requested filters and source data."
                ).strip()

            return {
                **analysis,
                "sql": sql,
                "explanation": explanation,
                "data": result_df,
                "success": True,
                "attempts": attempt + 1,
                "error": "",
            }

        except Exception as exc:
            last_error = str(exc)

            if attempt >= MAX_SQL_RETRIES:
                break

            try:
                repaired = repair_sql(
                    question=question,
                    schema=schema,
                    previous_sql=sql,
                    error=last_error,
                )

                repaired_sql = repaired.get(
                    "sql", ""
                ).strip()

                if not repaired_sql:
                    analysis.update(repaired)
                    last_error = (
                        repaired.get("explanation")
                        or last_error
                    )
                    break

                sql = repaired_sql

                for key in (
                    "chart_type",
                    "title",
                    "explanation",
                    "metric",
                    "dimension",
                ):
                    if key in repaired:
                        analysis[key] = repaired[key]

            except Exception as repair_exc:
                last_error = (
                    f"{last_error}\n"
                    f"SQL repair failed: {repair_exc}"
                )

                # Stop instead of repeating an identical failing prompt.
                break

    return {
        **analysis,
        "sql": sql,
        "data": pd.DataFrame(),
        "success": False,
        "attempts": min(
            MAX_SQL_RETRIES + 1,
            attempt + 1,
        ),
        "error": (
            "Query failed or could not be repaired.\n"
            f"Last error: {last_error}\n"
            f"Generated SQL: {sql}"
        ),
    }
