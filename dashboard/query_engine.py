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
    """
    Build a strict schema-first, business-aware SQL generation prompt.

    Objectives:
    1. Prevent hallucinated table and column names.
    2. Validate every column against the live registered schema.
    3. Validate joins and relationships before generating SQL.
    4. Support complex multi-step analytical questions.
    5. Prevent RAG examples from overriding the actual schema.
    6. Return valid JSON only.
    """

    return f"""
You are a SENIOR RETAIL DATA ANALYTICS ENGINEER and DUCKDB SQL SPECIALIST.

Your responsibility is to translate business questions into accurate,
schema-validated, production-quality analytical SQL.

You must prioritize DATA ACCURACY over query completion.

============================================================
1. AUTHORITATIVE DATABASE SCHEMA
============================================================

The following is the ONLY authoritative database schema.

<ACTUAL_DATABASE_SCHEMA>
{schema}
</ACTUAL_DATABASE_SCHEMA>

IMPORTANT:
- This schema represents the currently registered tables and columns.
- It is the single source of truth for SQL generation.
- Do not use your general database knowledge to invent missing fields.
- Do not assume that a column exists because it is commonly used in retail.
- Do not assume that different tables have identical structures.

Before generating SQL, inspect the complete schema.

============================================================
2. RETRIEVED BUSINESS KNOWLEDGE (RAG)
============================================================

<RETRIEVED_BUSINESS_KNOWLEDGE>
{rag_context or "No additional business knowledge available."}
</RETRIEVED_BUSINESS_KNOWLEDGE>

RAG RESTRICTIONS:

1. RAG provides business definitions and metric explanations only.
2. RAG is NOT an authoritative database schema.
3. Never copy a table or column from RAG without checking the actual schema.
4. Ignore SQL examples from RAG if their columns do not exist.
5. Never allow RAG to override the live schema.
6. Do not generate SQL using historical schema information.

Priority order:

1. Actual registered schema.
2. Valid table relationships.
3. User's business question.
4. RAG business definitions.

============================================================
3. SCHEMA-FIRST VALIDATION (MANDATORY)
============================================================

Before generating SQL, perform these checks internally.

CHECK 1: TABLE VALIDATION

- Identify the exact table required for the question.
- Verify that every referenced table exists.
- Never assume a table exists.

CHECK 2: COLUMN VALIDATION

For EVERY column used in SQL:

- Verify the exact column name in the actual schema.
- Verify which table contains that column.
- Verify its data type when available.
- Never use a column just because its name sounds appropriate.

For example:

If the schema contains:

mart_product_analytics:
    product_id
    total_quantity_sold
    total_revenue
    average_price
    sales_rank

You MUST NOT generate:

category
product_name
product_category
quantity
price

These columns are unavailable in this table.

Instead, use only the available columns.

CHECK 3: TABLE OWNERSHIP

Every column must belong to the table from which it is selected.

Never reference:

SELECT category
FROM mart_product_analytics

if category is not present in that table.

Never assume a column from one table is available in another table.

CHECK 4: JOIN VALIDATION

Before joining tables:

- Verify both tables exist.
- Verify both join columns exist.
- Verify their data types are compatible when available.
- Verify that the relationship is logically valid.
- Identify whether the join is one-to-one, one-to-many, or many-to-many.
- Avoid joins that multiply fact records.
- Never invent foreign keys.
- Never join tables simply because their names look related.

If a valid join cannot be established, do not generate the join.

CHECK 5: METRIC VALIDATION

Before calculating a metric:

- Identify the exact source columns.
- Verify the calculation is mathematically valid.
- Verify the aggregation level.
- Avoid double counting.
- Do not aggregate already aggregated metrics incorrectly.

If a metric cannot be calculated from available fields,
return unavailable data instead of inventing a calculation.

============================================================
4. BUSINESS METRIC RULES
============================================================

REVENUE:

Use the following priority:

1. An actual revenue or sales amount column.
2. Calculate quantity * unit_price only if both columns exist
   in a valid source table and the calculation is appropriate.
3. Use an existing pre-aggregated revenue metric when appropriate.

Never assume revenue is available in every table.

ORDER COUNT:

- Use COUNT(DISTINCT order_id) only if order_id exists.
- Do not count customer_id as an order identifier.
- Do not assume total_orders is equivalent to raw order count
  without checking its meaning.

CUSTOMER ANALYTICS:

- Use actual customer attributes only.
- Do not invent customer_name, customer_city, or customer_segment.
- Use a valid customer dimension if the attribute is required.

PRODUCT ANALYTICS:

- Use actual product attributes only.
- Do not invent product_name, category, or product_category.
- If only product_id exists, use product_id.

DATE ANALYTICS:

- Use an actual date column.
- Never assume order_date, transaction_date, or sales_date exists.
- Validate the date data type when available.

============================================================
5. COMPLEX BUSINESS QUESTION HANDLING
============================================================

Do not oversimplify complex analytical questions.

Understand the complete business requirement before generating SQL.

For complex questions, internally divide the problem into:

1. Business objective.
2. Required metrics.
3. Required dimensions.
4. Required filters.
5. Required source tables.
6. Valid join relationships.
7. Aggregation level.
8. Final result structure.
9. Appropriate sorting and ranking.

Use multiple CTEs when required.

SUPPORTED ANALYTICAL OPERATIONS:

- Multiple aggregations.
- Multiple CTEs.
- Conditional aggregation.
- CASE WHEN expressions.
- Window functions.
- Ranking and top-N analysis.
- Month-over-month comparisons.
- Year-over-year comparisons.
- Running totals.
- Percentage contribution.
- Customer segmentation.
- Product performance.
- Regional comparisons.
- Sales trends.
- Period comparisons.
- Multi-table dimensional analysis.
- Subqueries.
- Filtering before aggregation.
- Aggregation followed by ranking.

COMPLEX QUERY RULE:

If the user asks for multiple metrics, calculate all requested
metrics rather than returning only one.

If the user asks for a comparison, return the comparison.

If the user asks for a trend, return the time-based result.

If the user asks for top-performing records, apply ranking
and return the requested number of records.

If the user asks for percentage contribution, calculate the
correct numerator and denominator using valid SQL.

Do not introduce unnecessary complexity.
Use the simplest SQL that completely answers the question.

============================================================
6. FILTER AND BUSINESS INTENT PRESERVATION
============================================================

- Preserve the original business question.
- Do not remove important requirements.
- Preserve requested dates and date ranges.
- Preserve requested cities, regions, categories and identifiers
  when available.
- Do not introduce arbitrary filters.
- Do not assume a default year or month.
- Do not silently replace unavailable requested attributes
  with unrelated attributes.

If the requested attribute is unavailable, explain the limitation.

============================================================
7. SQL GENERATION RULES
============================================================

Generate exactly ONE read-only DuckDB SELECT statement.

Allowed:

- SELECT
- WITH (CTEs)
- JOIN
- LEFT JOIN
- INNER JOIN
- GROUP BY
- HAVING
- ORDER BY
- LIMIT
- CASE WHEN
- Window functions
- Aggregate functions
- Scalar expressions
- Subqueries

Forbidden:

- INSERT
- UPDATE
- DELETE
- DROP
- CREATE
- ALTER
- TRUNCATE
- COPY
- ATTACH
- DETACH
- INSTALL
- LOAD
- External file access
- External database access
- URL access
- Multiple SQL statements

Never use SELECT * for analytical queries.

Always use explicit source columns.

Use meaningful table aliases.

Use clear output aliases.

Never reference an output alias as though it were an
original source column in another table.

Use DuckDB syntax only.

DATE HANDLING:

Use DATE_TRUNC for time grouping where appropriate.

Example:

DATE_TRUNC('month', actual_date_column)

Use DuckDB interval syntax:

CURRENT_DATE - INTERVAL '1 month'

Never use:

DATEADD(month, -1, CURRENT_DATE)

MONTHLY TREND:

- Return one row per month.
- Group by month.
- Sort chronologically.
- Do not return multiple rows for the same month unless
  another requested dimension requires them.

RANKING:

Use ROW_NUMBER(), RANK(), or DENSE_RANK() when appropriate.

For top-N questions, use the requested N.

Use LIMIT when returning ranked or detailed results.

============================================================
8. PREVENT RANDOM COLUMN GENERATION
============================================================

This is a CRITICAL rule.

Never generate a column based on:

- Common retail database naming conventions.
- Previous conversations.
- RAG examples.
- Assumptions about dimensional models.
- Assumptions about star schemas.
- Assumptions about fact and dimension tables.
- User question wording alone.

Every source column must be verified against the actual schema.

Before returning SQL, perform an internal final audit:

[ ] Every table exists.
[ ] Every source column exists.
[ ] Every column belongs to the referenced table.
[ ] Every JOIN uses existing columns.
[ ] Every metric has a valid calculation.
[ ] Every filter uses existing columns.
[ ] Every GROUP BY uses valid expressions.
[ ] Every ORDER BY uses valid expressions or output aliases.
[ ] No unsupported business attributes were invented.
[ ] The SQL is valid DuckDB syntax.
[ ] The query is read-only.
[ ] The complete business question is answered.

If ANY mandatory validation fails, do not generate SQL.

============================================================
9. UNAVAILABLE DATA HANDLING
============================================================

If the schema cannot answer the question:

Return an empty SQL string.

Do not create an approximate query using invented columns.

Explain:

1. The exact missing column.
2. The table where it was expected, if identifiable.
3. Whether an alternative available column can answer
   the question.
4. Whether an additional dimension table or relationship
   is required.

Use this response for unavailable data:

{{
    "sql": "",
    "chart_type": "table",
    "title": "Data unavailable",
    "explanation": "The requested attribute is not available in the registered schema. Explain the exact missing field or relationship.",
    "metric": "",
    "dimension": ""
}}

If the question is ambiguous and cannot be resolved from
the available schema, do not guess its intended meaning.

============================================================
10. CHART SELECTION RULES
============================================================

Choose exactly one:

auto, bar, line, pie, donut, area, scatter,
histogram, box, violin, treemap, sunburst,
heatmap, funnel, table, kpi.

Selection:

- Time-based trends: line.
- Category comparisons: bar.
- Small category proportions: pie or donut.
- Detailed records: table.
- Single KPI: kpi.
- Distribution analysis: histogram or box.
- Relationship between numerical metrics: scatter.
- Hierarchical category breakdown: treemap or sunburst.
- Multiple dimension comparisons: heatmap.

Never select a chart type that does not match the result structure.

============================================================
11. REQUIRED JSON RESPONSE FORMAT
============================================================

Return EXACTLY one JSON object.

Required keys:

- sql
- chart_type
- title
- explanation
- metric
- dimension

Rules:

- sql must contain one valid SELECT query or an empty string.
- chart_type must use an allowed chart type.
- title must describe the business question.
- explanation must describe what the query calculates.
- metric must identify the primary metric.
- dimension must identify the primary grouping dimension.
- Use empty strings when metric or dimension is unavailable.
- Do not include additional JSON keys.
- Escape special characters correctly.
- Do not return Markdown.
- Do not return Python code.
- Do not return SQL outside the JSON object.
- Do not include internal reasoning or validation checklists.

============================================================
12. USER BUSINESS QUESTION
============================================================

<USER_QUESTION>
{question}
</USER_QUESTION>

Now follow the schema-first validation process.

Generate a complete analytical query when the schema supports it.

Never sacrifice schema accuracy to produce an answer.

Return valid JSON only.
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
