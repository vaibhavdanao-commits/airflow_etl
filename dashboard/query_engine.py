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


# ============================================================
# CONFIGURATION
# ============================================================

MAX_RESULT_ROWS = max(1, int(os.getenv("MAX_RESULT_ROWS", "500")))
# Retry twice by default after the first failed execution.
MAX_SQL_RETRIES = max(0, int(os.getenv("MAX_SQL_RETRIES", "2")))
LLM_TIMEOUT_SECONDS = max(10, int(os.getenv("LLM_TIMEOUT_SECONDS", "120")))
LLM_MAX_TOKENS = max(256, int(os.getenv("LLM_MAX_TOKENS", "800")))
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.0"))

ALLOWED_CHARTS = {
    "auto", "bar", "line", "pie", "donut", "area", "scatter",
    "histogram", "box", "violin", "treemap", "sunburst",
    "heatmap", "funnel", "table", "kpi",
}

BLOCKED_FUNCTIONS = {
    "READ_CSV", "READ_CSV_AUTO", "READ_PARQUET", "READ_JSON",
    "READ_JSON_AUTO", "PARQUET_SCAN", "CSV_SCAN", "JSON_SCAN",
    "GLOB", "HTTPFS", "READ_TEXT", "READ_BLOB", "READ_NPY",
    "READ_XLS", "READ_XLSX", "QUERY", "QUERY_TABLE", "EXECUTE",
    "INSTALL", "LOAD", "FORCE_CHECKPOINT", "READ_TEXT",
}

FORBIDDEN_EXPRESSIONS = (
    "Insert", "Update", "Delete", "Drop", "Create", "Command",
    "Copy", "Pragma", "Attach", "Detach", "Transaction", "Merge",
    "Into", "Grant", "Revoke", "Use", "Set", "Analyze",
    "TruncateTable", "Alter", "Call", "Export", "Import",
)


# ============================================================
# OLLAMA CONNECTION
# ============================================================

def make_llm() -> ChatOpenAI:
    """Connect to local Ollama through its OpenAI-compatible endpoint."""

    base_url = os.getenv(
        "OLLAMA_BASE_URL",
        "http://localhost:11434/v1",
    ).strip().rstrip("/")

    model = os.getenv(
        "OLLAMA_MODEL",
        "qwen2.5-coder:7b",
    ).strip()

    if not model:
        raise ValueError(
            "OLLAMA_MODEL is empty. Check your dashboard .env file."
        )

    parsed = urlparse(base_url)

    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(
            "Invalid OLLAMA_BASE_URL. Use http://localhost:11434/v1."
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


def response_text(response: Any) -> str:
    """Convert a LangChain response to plain text."""

    content = getattr(response, "content", response)

    if isinstance(content, str):
        return content

    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                parts.append(str(item.get("text", "")))
            else:
                parts.append(str(item))
        return "".join(parts)

    return str(content)


def invoke_llm(system_prompt: str, user_prompt: str) -> str:
    """Invoke the configured local Ollama model."""

    try:
        response = make_llm().invoke(
            [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_prompt),
            ]
        )

        result = response_text(response)

        if not result.strip():
            raise ValueError("Ollama returned an empty response.")

        return result

    except Exception as exc:
        message = str(exc)
        lowered = message.lower()

        if "connection" in lowered or "connection refused" in lowered:
            raise RuntimeError(
                "Cannot connect to Ollama. Start Ollama and verify "
                "OLLAMA_BASE_URL and OLLAMA_MODEL in your .env file."
            ) from exc

        if (
            "token repeat limit reached" in lowered
            or "prediction aborted" in lowered
        ):
            raise RuntimeError(
                "Ollama aborted generation. Try a shorter prompt, verify "
                "the model with 'ollama list', or use another local model."
            ) from exc

        raise RuntimeError(f"Ollama request failed: {message}") from exc


# ============================================================
# JSON PARSING
# ============================================================

def extract_json(text: str) -> dict[str, Any]:
    """Extract a JSON object from a model response."""

    if not text or not text.strip():
        raise ValueError("Ollama returned an empty response.")

    cleaned = text.strip()

    # Remove optional Markdown fences.
    cleaned = re.sub(
        r"^\s*```(?:json)?\s*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s*```\s*$", "", cleaned)

    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    # Find the first valid JSON object embedded in explanatory text.
    decoder = json.JSONDecoder()

    for match in re.finditer(r"\{", cleaned):
        try:
            parsed, _ = decoder.raw_decode(cleaned[match.start():])
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            continue

    # Compatibility: accept a plain SQL code fence.
    sql_match = re.search(
        r"```(?:sql)?\s*(.*?)```",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )

    if sql_match:
        candidate = sql_match.group(1).strip()

        if re.match(r"^(SELECT|WITH)\b", candidate, flags=re.IGNORECASE):
            return {
                "sql": candidate,
                "chart_type": "auto",
                "title": "Analysis Results",
                "explanation": "SQL extracted from the model response.",
                "metric": "",
                "dimension": "",
            }

    # Compatibility: accept a plain SELECT/WITH response.
    if re.match(r"^(SELECT|WITH)\b", cleaned, flags=re.IGNORECASE):
        return {
            "sql": cleaned,
            "chart_type": "auto",
            "title": "Analysis Results",
            "explanation": "",
            "metric": "",
            "dimension": "",
        }

    raise ValueError(
        "Ollama did not return valid JSON or recognizable SQL. "
        f"Response received: {cleaned[:500]}"
    )


# ============================================================
# LIVE WAREHOUSE SCHEMA
# ============================================================

def schema_description(frames: dict[str, pd.DataFrame]) -> str:
    """
    Describe the actual DataFrame columns registered by the dashboard.

    This function intentionally avoids inventing business-friendly
    columns that are not present in the loaded tables.
    """

    chunks: list[str] = []

    for table_name, df in frames.items():
        chunks.append(f'TABLE "{table_name}" ({len(df):,} rows)')

        for column, dtype in zip(df.columns, df.dtypes):
            chunks.append(f'  - "{column}" ({dtype})')

    return "\n".join(chunks)


def live_schema_description(
    connection: duckdb.DuckDBPyConnection,
    frames: dict[str, pd.DataFrame],
) -> str:
    """
    Read the real registered DuckDB table columns.

    DuckDB DESCRIBE is the primary source. DataFrame metadata is used
    only as a fallback if a table cannot be described.
    """

    chunks: list[str] = []

    for table_name, frame in frames.items():
        escaped_name = table_name.replace('"', '""')

        try:
            description = connection.execute(
                f'DESCRIBE "{escaped_name}"'
            ).fetchdf()

            columns = [
                f'  - "{row["column_name"]}" ({row["column_type"]})'
                for _, row in description.iterrows()
            ]

        except Exception:
            columns = [
                f'  - "{column}" ({dtype})'
                for column, dtype in zip(frame.columns, frame.dtypes)
            ]

        chunks.append(
            f'TABLE "{table_name}"\n' + "\n".join(columns)
        )

    return "\n\n".join(chunks)


# ============================================================
# SQL PROMPTS AND ANALYSIS
# ============================================================

def build_system_prompt(
    schema: str,
    question: str,
    rag_context: str = "",
) -> str:
    """Build a strict schema-aware SQL-generation prompt."""

    return f"""
You are a careful retail analytics engineer writing DuckDB SQL.

Return exactly one JSON object with these keys:
sql, chart_type, title, explanation, metric, dimension.

ACTUAL REGISTERED TABLES AND COLUMNS:
{schema}

RETRIEVED BUSINESS KNOWLEDGE (RAG):
{rag_context or "No additional business context was retrieved."}

RAG RULES:
- Use retrieved knowledge for business definitions, metric meanings, and examples.
- The live registered schema above is authoritative for table and column names.
- Ignore any retrieved example that mentions a table, column, or join not present
  in the live schema. Never treat RAG text as permission to invent SQL fields.

STRICT SCHEMA RULES:
1. Use only tables and columns explicitly listed in the schema above.
2. Never assume a column exists because it is common in retail databases.
3. Never invent a table, column, identifier, metric, or join relationship.
4. Before writing SQL, identify the exact table containing each required column.
5. A column in one table cannot be referenced from another table unless a
   valid join connects those tables.
6. Join tables only with matching key columns that are both present in the schema.
7. Do not create joins that multiply fact rows.
8. If a requested attribute is unavailable, do not fabricate it. Use an available
   identifier if that still answers the question, or return sql="" and explain
   exactly which field or relationship is missing.
9. Do not use a display alias as a source column. For example, an output alias
   named product_name does not mean that product_name exists in the source table.
10. If mart_product_analytics contains product_id but not product_name, select
    product_id unless a separate dimension table has a real product_name column
    and a valid join key is present.

METRIC RULES:
- Use an actual sales/revenue amount column when available.
- Otherwise calculate quantity * price only when both columns exist and the
  relationship is valid.
- Order count requires a real order identifier.
- Do not fabricate revenue, order counts, customer attributes, or dates.

QUERY RULES:
- Generate exactly one read-only SELECT query. CTEs are allowed.
- Do not use SELECT * for analytical aggregation.
- Preserve the user's filters, identifiers, city, category, and date range.
- Do not add arbitrary date filters.
- For monthly trends, return one row per month and sort chronologically.
- Use clear output aliases, but only use real source columns in expressions.
- Use DuckDB SQL syntax only; do not use SQL Server, MySQL, or BigQuery-specific syntax.
- Never use DATEADD(unit, amount, date). DuckDB date arithmetic must use
  date_expression + INTERVAL 'N unit' or
  date_expression - INTERVAL 'N unit'. Example:
  CURRENT_DATE - INTERVAL '1 month'.
- Do not read files, access URLs, or use external databases.
- Never generate write or administrative statements.
- Use LIMIT when returning ranked or detailed results.

QUESTION:
{question}

CHART TYPE:
Choose one: auto, bar, line, pie, donut, area, scatter, histogram,
box, violin, treemap, sunburst, heatmap, funnel, table, kpi.

Use line for trends, bar for comparisons, pie/donut for a small number
of categories, table for detail, and kpi for a single value.

If the schema cannot answer the question, return:
{{"sql":"","chart_type":"table","title":"Unavailable data",
"explanation":"Explain the exact missing column or relationship.",
"metric":"","dimension":""}}

Return valid JSON only. No Markdown fences. Keep the response concise.
""".strip()


def normalize_analysis(result: dict[str, Any]) -> dict[str, Any]:
    """Ensure consistent analysis output fields."""

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

    for key in ("title", "explanation", "metric", "dimension"):
        normalized[key] = str(
            normalized[key] or ""
        ).strip()

    return normalized


def generate_analysis(
    question: str,
    frames: dict[str, pd.DataFrame],
    schema: str | None = None,
    rag_context: str = "",
) -> dict[str, Any]:
    """
    Generate one schema-aware SQL analysis.

    `schema` is optional for backward compatibility. The main execution
    flow passes the live DuckDB schema explicitly.
    """

    if not question or not question.strip():
        raise ValueError("Please enter a business question.")

    if not frames:
        raise ValueError("No warehouse tables were loaded.")

    actual_schema = schema or schema_description(frames)

    system_prompt = build_system_prompt(
        schema=actual_schema,
        question=question.strip(),
        rag_context=rag_context,
    )

    raw = invoke_llm(
        system_prompt,
        "Generate SQL analysis using only the supplied schema.\n"
        f"Business question: {question.strip()}",
    )

    return normalize_analysis(
        extract_json(raw)
    )


def generate_related_questions(
    question: str,
    frames: dict[str, pd.DataFrame],
    max_related: int = 4,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> list[str]:
    """
    Generate related business questions using local Ollama.

    The optional connection allows the planner to use live DuckDB schema.
    Existing calls with three arguments remain compatible.
    """

    max_related = max(0, min(int(max_related), 8))

    if max_related == 0:
        return []

    if connection is not None:
        schema = live_schema_description(connection, frames)
    else:
        schema = schema_description(frames)

    system_prompt = """
You are a retail analytics planner. Generate related analytical questions
for a user's main business question. Use only the supplied warehouse schema.

Return valid JSON only in this form:
{"questions":["question 1","question 2"]}

Do not return SQL. Do not invent unavailable columns or relationships.
Keep each question specific, useful, distinct, and related to the original
question. Preserve important filters such as product IDs, city, category,
and date range when relevant. Do not repeat the original question.
""".strip()

    user_prompt = f"""
Original question:
{question}

Available warehouse schema:
{schema}

Generate {max_related} related questions that can be answered from this schema.
Favor complementary analyses such as time trends, product/category breakdowns,
top performers, customer/location comparisons, and segment analysis only when
the required fields and relationships exist.

Return JSON only.
""".strip()

    raw = invoke_llm(
        system_prompt,
        user_prompt,
    )

    payload = extract_json(raw)
    candidates = payload.get("questions", [])

    if not isinstance(candidates, list):
        return []

    seen = {question.strip().casefold()}
    questions: list[str] = []

    for candidate in candidates:
        if not isinstance(candidate, str):
            continue

        candidate = candidate.strip()

        if not candidate or candidate.casefold() in seen:
            continue

        seen.add(candidate.casefold())
        questions.append(candidate)

        if len(questions) >= max_related:
            break

    return questions


# ============================================================
# DUCKDB DIALECT NORMALIZATION
# ============================================================

def normalize_duckdb_dateadd(sql: str) -> str:
    """Convert common SQL Server DATEADD(unit, amount, date) syntax to DuckDB.

    Example:
        DATEADD(month, -1, CURRENT_DATE)
    becomes:
        CURRENT_DATE - INTERVAL '1 month'

    This intentionally handles integer offsets and simple date expressions.
    More complex expressions are left unchanged for DuckDB validation/LLM repair.
    """
    pattern = re.compile(
        r"\bDATEADD\s*\(\s*"
        r"(year|quarter|month|week|day|hour|minute|second)\s*,\s*"
        r"(-?\d+)\s*,\s*"
        r"((?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)*"
        r"[A-Za-z_][A-Za-z0-9_]*|CURRENT_DATE|CURRENT_TIMESTAMP)"
        r"\s*\)",
        flags=re.IGNORECASE,
    )

    def replace(match: re.Match[str]) -> str:
        unit = match.group(1).lower()
        amount = int(match.group(2))
        date_expression = match.group(3).strip()

        # Normalize common SQL Server datepart aliases.
        unit = {"yy": "year", "qq": "quarter", "mm": "month", "ww": "week", "dd": "day"}.get(unit, unit)

        if amount == 0:
            return date_expression

        operator = "+" if amount > 0 else "-"
        return f"({date_expression} {operator} INTERVAL '{abs(amount)} {unit}')"

    return pattern.sub(replace, sql)


# ============================================================
# SQL VALIDATION
# ============================================================

def validate_sql(
    sql: str,
    available_tables: list[str],
) -> str:
    """
    Allow one read-only SELECT query against registered warehouse tables.

    This checks statement type, external/file functions, and table names.
    DuckDB EXPLAIN in run_read_only_query performs the actual column-binding
    check against the live registered tables.
    """

    if not sql or not sql.strip():
        raise ValueError(
            "The AI did not generate SQL. The requested data may be unavailable."
        )

    sql = sql.strip().rstrip(";")
    # Correct a common model-generated SQL Server DATEADD expression before
    # parsing and executing the query in DuckDB.
    sql = normalize_duckdb_dateadd(sql)

    blocked_pattern = (
        r"\b(?:"
        + "|".join(re.escape(name) for name in BLOCKED_FUNCTIONS)
        + r")\s*\("
    )

    if re.search(blocked_pattern, sql, flags=re.IGNORECASE):
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
        raise ValueError("Only read-only SELECT queries are allowed.")

    for expression_name in FORBIDDEN_EXPRESSIONS:
        expression_type = getattr(exp, expression_name, None)

        if expression_type is not None and tree.find(expression_type):
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

        # Do not permit catalog/schema-qualified or external references.
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
                f"Unknown table '{table.name}'. Available tables: "
                f"{', '.join(available_tables)}"
            )

    return tree.sql(dialect="duckdb")


# ============================================================
# READ-ONLY SQL EXECUTION
# ============================================================

def run_read_only_query(
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    available_tables: list[str],
) -> pd.DataFrame:
    """
    Validate, bind-check, and execute a read-only query.

    EXPLAIN makes DuckDB resolve table and column references before the
    actual query is executed. Missing columns are therefore caught early
    and can be sent to the automatic SQL repair function.
    """

    safe_sql = validate_sql(
        sql,
        available_tables,
    )

    try:
        # Validate actual table and column bindings against DuckDB.
        connection.execute(
            f"EXPLAIN {safe_sql}"
        )

        bounded_sql = (
            f"SELECT * FROM ({safe_sql}) AS dashboard_result "
            f"LIMIT {MAX_RESULT_ROWS}"
        )

        return connection.execute(
            bounded_sql
        ).fetchdf()

    except Exception as exc:
        raise RuntimeError(
            f"SQL execution failed: {exc}\n\n"
            f"Generated SQL:\n{safe_sql}"
        ) from exc


# ============================================================
# AUTOMATIC SQL REPAIR
# ============================================================

def repair_sql(
    question: str,
    schema: str,
    previous_sql: str,
    error: str,
) -> dict[str, Any]:
    """Ask Ollama to repair failed SQL using the actual live schema."""

    system_prompt = f"""
You repair DuckDB SELECT queries for a retail analytics dashboard.

ACTUAL REGISTERED TABLES AND COLUMNS:
{schema}

STRICT REPAIR RULES:
1. Use only the tables and columns listed above.
2. Fix the specific DuckDB error shown by the user.
3. Preserve the original business intent and filters.
4. Never invent a missing column or join key.
5. Do not assume product_name exists in a product mart unless it is listed.
6. If product_id exists but product_name does not, use product_id unless
   a real product dimension table and valid join key are listed.
7. Do not use output aliases as source columns.
8. Use DuckDB SQL syntax only. Never use DATEADD(unit, amount, date).
   Use date_expression - INTERVAL '1 month' or
   date_expression + INTERVAL '7 days' instead.
9. Return one read-only SELECT query, or sql="" if the schema cannot answer.
10. Return JSON only with keys:
   sql, chart_type, title, explanation, metric, dimension.
""".strip()

    user_prompt = f"""
Business question:
{question}

Previous SQL:
{previous_sql}

DuckDB validation/execution error:
{error}

Correct the SQL using only the actual schema.
If the schema cannot support the request, return an empty SQL string
and explain the missing column or relationship.
Return JSON only.
""".strip()

    return normalize_analysis(
        extract_json(
            invoke_llm(
                system_prompt,
                user_prompt,
            )
        )
    )


# ============================================================
# FULL GENERATION, VALIDATION, REPAIR AND EXECUTION
# ============================================================

def generate_and_run(
    question: str,
    frames: dict[str, pd.DataFrame],
    connection: duckdb.DuckDBPyConnection,
    rag_context: str = "",
) -> dict[str, Any]:
    """
    Generate SQL, validate it against live DuckDB metadata, execute it,
    and automatically repair failures.

    Returns the analysis fields plus:
        data: pandas DataFrame
        success: bool
        attempts: int
        error: str
    """

    if not question or not question.strip():
        raise ValueError("Please enter a business question.")

    if not frames:
        raise ValueError("No warehouse tables were loaded.")

    available_tables = list(frames.keys())

    # IMPORTANT:
    # Build the live schema first and pass that exact schema to Ollama.
    # This avoids generating SQL from an outdated or incomplete schema.
    schema = live_schema_description(
        connection,
        frames,
    )

    analysis = generate_analysis(
        question=question,
        frames=frames,
        schema=schema,
        rag_context=rag_context,
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
    attempts_used = 0

    for attempt in range(MAX_SQL_RETRIES + 1):
        attempts_used = attempt + 1

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
                "attempts": attempts_used,
                "error": "",
            }

        except Exception as exc:
            last_error = str(exc)

            # No more retries remaining.
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
                    "sql",
                    "",
                ).strip()

                if not repaired_sql:
                    explanation = repaired.get("explanation", "")
                    if explanation:
                        last_error = explanation
                    break

                # Prevent repeatedly executing the same failed SQL.
                if repaired_sql.rstrip(";").strip() == sql.rstrip(";").strip():
                    last_error = (
                        f"{last_error}\n"
                        "SQL repair returned the same query. "
                        "Stopping to avoid repeating the same failure."
                    )
                    break

                sql = repaired_sql

                # Keep the latest chart metadata returned by the repair model.
                for key in (
                    "chart_type",
                    "title",
                    "explanation",
                    "metric",
                    "dimension",
                ):
                    if repaired.get(key):
                        analysis[key] = repaired[key]

            except Exception as repair_exc:
                last_error = (
                    f"{last_error}\n"
                    f"SQL repair failed: {repair_exc}"
                )
                break

    return {
        **analysis,
        "sql": sql,
        "data": pd.DataFrame(),
        "success": False,
        "attempts": attempts_used,
        "error": (
            "Query failed or could not be repaired automatically.\n"
            f"Last error: {last_error}\n"
            f"Last generated SQL: {sql}"
        ),
    }
