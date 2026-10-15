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
MAX_SQL_RETRIES = max(0, int(os.getenv("MAX_SQL_RETRIES", "1")))
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
# HUGGING FACE INFERENCE API CONNECTION
# ============================================================

def make_llm() -> ChatOpenAI:
    """Connect to Hugging Face Inference Providers through its OpenAI-compatible API."""
    base_url = os.getenv(
        "HF_BASE_URL", "https://router.huggingface.co/v1"
    ).strip().rstrip("/")
    model = os.getenv(
        "HF_MODEL", "Qwen/Qwen2.5-Coder-32B-Instruct:fastest"
    ).strip()
    api_key = os.getenv("HF_TOKEN", "").strip()

    if not api_key:
        raise ValueError(
            "HF_TOKEN is missing. Create a Hugging Face token with "
            "Inference Providers permission and add it to your dashboard .env file."
        )
    if not model:
        raise ValueError("HF_MODEL is empty. Check your dashboard .env file.")

    parsed = urlparse(base_url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError(
            "Invalid HF_BASE_URL. Use https://router.huggingface.co/v1."
        )
    if parsed.path in ("", "/"):
        base_url += "/v1"

    return ChatOpenAI(
        model=model,
        base_url=base_url,
        api_key=api_key,
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
    """Invoke the Hugging Face chat-completion API and return response text."""
    try:
        response = make_llm().invoke(
            [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_prompt),
            ]
        )
        text = response_text(response)
        if not text.strip():
            raise ValueError("Hugging Face returned an empty response.")
        return text

    except Exception as exc:
        message = str(exc)
        lowered = message.lower()

        if any(term in lowered for term in ("401", "unauthorized", "invalid api key", "authentication")):
            raise RuntimeError(
                "Hugging Face authentication failed. Check HF_TOKEN and confirm "
                "it has Inference Providers permission."
            ) from exc

        if any(term in lowered for term in ("404", "model_not_found", "not found")):
            raise RuntimeError(
                "The configured Qwen model may not be available through the selected "
                "Hugging Face provider. Check HF_MODEL and the Hugging Face model page."
            ) from exc

        if any(term in lowered for term in ("429", "rate limit", "quota", "too many requests")):
            raise RuntimeError(
                "Hugging Face rate limit or inference credits were reached. Check "
                "your Inference Providers usage and try again later."
            ) from exc

        if "timeout" in lowered:
            raise RuntimeError(
                "Hugging Face inference timed out. Try a smaller Qwen model, a shorter "
                "question/schema prompt, or a higher LLM_TIMEOUT_SECONDS value."
            ) from exc

        raise RuntimeError(f"Hugging Face inference request failed: {message}") from exc


# ============================================================
# JSON PARSING
# ============================================================

def extract_json(text: str) -> dict[str, Any]:
    """Extract a JSON object, including when the model wraps it in fences."""
    if not text or not text.strip():
        raise ValueError("Hugging Face returned an empty response.")

    cleaned = text.strip()
    cleaned = re.sub(
        r"^\s*```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE
    )
    cleaned = re.sub(r"\s*```\s*$", "", cleaned)

    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", cleaned):
        try:
            parsed, _ = decoder.raw_decode(cleaned[match.start():])
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            continue

    sql_match = re.search(
        r"```(?:sql)?\s*(.*?)```", text,
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

    raise ValueError(
        "The Hugging Face model did not return valid JSON or recognizable SQL. "
        f"Response received: {cleaned[:500]}"
    )


# ============================================================
# SCHEMA DESCRIPTION
# ============================================================

def schema_description(frames: dict[str, pd.DataFrame]) -> str:
    """Describe real tables and columns, with a few categorical examples."""
    chunks: list[str] = []

    for name, df in frames.items():
        chunks.append(f'\nTABLE "{name}" ({len(df):,} rows)')
        for column in df.columns:
            series = df[column]
            details = f'  "{column}" ({series.dtype})'

            if (
                pd.api.types.is_object_dtype(series)
                or isinstance(series.dtype, pd.CategoricalDtype)
            ):
                examples = (
                    series.dropna().astype(str).drop_duplicates().head(3).tolist()
                )
                if examples:
                    details += "; examples=" + json.dumps(
                        examples, ensure_ascii=False, default=str
                    )

            chunks.append(details)

    return "\n".join(chunks)


def live_schema_description(
    connection: duckdb.DuckDBPyConnection,
    frames: dict[str, pd.DataFrame],
) -> str:
    """Read live registered table metadata, falling back to DataFrame columns."""
    chunks = []

    for table_name, frame in frames.items():
        escaped = table_name.replace('"', '""')
        try:
            desc = connection.execute(f'DESCRIBE "{escaped}"').fetchdf()
            columns = [
                f'- "{row["column_name"]}" ({row["column_type"]})'
                for _, row in desc.iterrows()
            ]
        except Exception:
            columns = [
                f'- "{column}" ({dtype})'
                for column, dtype in zip(frame.columns, frame.dtypes)
            ]

        chunks.append(f'TABLE "{table_name}"\n' + "\n".join(columns))

    return "\n\n".join(chunks)


# ============================================================
# SQL PROMPTS AND ANALYSIS
# ============================================================

def build_system_prompt(schema: str, question: str) -> str:
    """Build a compact schema-aware SQL prompt."""
    return f"""
You are a careful retail analytics engineer writing DuckDB SQL.

Return exactly one JSON object with keys:
sql, chart_type, title, explanation, metric, dimension.

ACTUAL REGISTERED TABLES AND COLUMNS:
{schema}

SCHEMA RULES:
- Use only tables and columns listed above.
- Never invent columns, tables, keys, metrics, or join relationships.
- A dimension column cannot be referenced as if it were in fact_sales.
- Join dimension tables only using matching keys supported by the schema.
- Do not create joins that multiply fact rows.
- If a necessary column or relationship is missing, return sql="" and
  explain the exact missing field or relationship.
- Verify a requested attribute such as payment_method or order_status exists.

METRIC RULES:
- Use an actual sales/revenue amount column when available.
- Otherwise calculate quantity * price only when both columns and a valid
  relationship exist and the calculation is meaningful.
- Order count requires a real order identifier.
- Do not fabricate revenue, order counts, or customer attributes.

QUERY RULES:
- Generate one read-only SELECT query; CTEs are allowed.
- Do not use SELECT * for analytical aggregation.
- Preserve user filters, product identifiers, city, and date ranges.
- Do not add arbitrary date filters.
- For monthly trends, return one row per month and sort chronologically.
- Use clear aliases such as month, product_name, total_sales.
- Use DuckDB SQL syntax.
- Do not read files, access URLs, or use external databases.
- Never generate write or administrative statements.

QUESTION:
{question}

CHART TYPE:
Choose one: auto, bar, line, pie, donut, area, scatter, histogram,
box, violin, treemap, sunburst, heatmap, funnel, table, kpi.

Use line for trends, bar for comparisons, pie/donut for a small number
of categories, table for detail, and kpi for a single value.

If unsupported, return:
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

    normalized["sql"] = str(normalized["sql"] or "").strip().rstrip(";")
    normalized["chart_type"] = str(
        normalized["chart_type"] or "auto"
    ).strip().lower()

    if normalized["chart_type"] not in ALLOWED_CHARTS:
        normalized["chart_type"] = "auto"

    for key in ("title", "explanation", "metric", "dimension"):
        normalized[key] = str(normalized[key] or "").strip()

    return normalized


def generate_analysis(
    question: str,
    frames: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    """Generate one schema-aware SQL analysis."""
    if not question or not question.strip():
        raise ValueError("Please enter a business question.")
    if not frames:
        raise ValueError("No warehouse tables were loaded.")

    schema = schema_description(frames)
    system_prompt = build_system_prompt(schema=schema, question=question.strip())
    raw = invoke_llm(
        system_prompt,
        "Generate SQL analysis using only the supplied schema.\n"
        f"Business question: {question.strip()}",
    )
    return normalize_analysis(extract_json(raw))


def generate_related_questions(
    question: str,
    frames: dict[str, pd.DataFrame],
    max_related: int = 4,
) -> list[str]:
    """
    Generate related business questions using the same Hugging Face Qwen model.
    Each returned question should be passed separately to generate_analysis().
    """
    max_related = max(0, min(int(max_related), 8))
    if max_related == 0:
        return []

    schema = schema_description(frames)
    system_prompt = """
You are a retail analytics planner. Generate related analytical questions
for a user's main business question. Use only the supplied warehouse schema.
Return valid JSON only in the form {"questions":["question 1", "question 2"]}.
Do not return SQL. Do not invent unavailable columns. Keep each question
specific, useful, distinct, and related to the original question. Preserve
important filters such as product IDs, city, category, and date range when
relevant. Do not repeat the original question.
""".strip()

    user_prompt = f"""
Original question:
{question}

Available warehouse schema:
{schema}

Generate {max_related} related questions that can be answered from this schema.
Favor complementary analyses such as time trends, product/category breakdowns,
top performers, customer/location comparisons, and segment analysis when the
required fields exist. Return JSON only.
""".strip()

    raw = invoke_llm(system_prompt, user_prompt)
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
# SQL VALIDATION
# ============================================================

def validate_sql(sql: str, available_tables: list[str]) -> str:
    """Allow one read-only SELECT query against registered warehouse tables."""
    if not sql or not sql.strip():
        raise ValueError(
            "The AI did not generate SQL. The requested data may be unavailable."
        )

    sql = sql.strip().rstrip(";")
    blocked_pattern = (
        r"\b(?:" + "|".join(re.escape(name) for name in BLOCKED_FUNCTIONS)
        + r")\s*\("
    )
    if re.search(blocked_pattern, sql, flags=re.IGNORECASE):
        raise ValueError("External or file-reading SQL functions are prohibited.")

    try:
        statements = sqlglot.parse(sql, read="duckdb")
    except Exception as exc:
        raise ValueError(f"Generated SQL could not be parsed: {exc}") from exc

    if len(statements) != 1 or statements[0] is None:
        raise ValueError("Only one SQL statement is allowed.")

    tree = statements[0]
    if not isinstance(tree, (exp.Select, exp.Union)):
        raise ValueError("Only read-only SELECT queries are allowed.")

    for expression_name in FORBIDDEN_EXPRESSIONS:
        expression_type = getattr(exp, expression_name, None)
        if expression_type is not None and tree.find(expression_type):
            raise ValueError("The generated SQL contains a prohibited operation.")

    for node in tree.walk():
        if isinstance(node, exp.Func):
            function_name = node.sql_name().upper()
            if function_name in BLOCKED_FUNCTIONS:
                raise ValueError(f"Function '{function_name}' is prohibited.")

    allowed_tables = {table.lower() for table in available_tables}
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

        if table_name not in allowed_tables and table_name not in cte_names:
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
    """Validate and execute a query with a bounded result."""
    safe_sql = validate_sql(sql, available_tables)
    bounded_sql = (
        f"SELECT * FROM ({safe_sql}) AS dashboard_result "
        f"LIMIT {MAX_RESULT_ROWS}"
    )

    try:
        return connection.execute(bounded_sql).fetchdf()
    except Exception as exc:
        raise RuntimeError(
            f"SQL execution failed: {exc}\n\nGenerated SQL:\n{safe_sql}"
        ) from exc


# ============================================================
# SQL REPAIR AND FULL EXECUTION
# ============================================================

def repair_sql(
    question: str,
    schema: str,
    previous_sql: str,
    error: str,
) -> dict[str, Any]:
    """Ask the Hugging Face Qwen model to repair a failed query using the actual schema."""
    system_prompt = f"""
You repair DuckDB SELECT queries for a retail analytics dashboard.

ACTUAL TABLES AND COLUMNS:
{schema}

Rules:
- Use only listed tables and columns.
- Fix the specific DuckDB error.
- Preserve the user's business intent and filters.
- Never invent missing columns or join keys.
- Return one read-only SELECT query or sql="" if impossible.
- Return JSON only with keys: sql, chart_type, title, explanation, metric, dimension.
""".strip()

    user_prompt = f"""
Business question: {question}

Previous SQL:
{previous_sql}

DuckDB error:
{error}

Return corrected SQL as JSON. If the schema cannot support the question,
return an empty SQL string and explain why.
""".strip()

    return normalize_analysis(
        extract_json(invoke_llm(system_prompt, user_prompt))
    )


def generate_and_run(
    question: str,
    frames: dict[str, pd.DataFrame],
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, Any]:
    """
    Generate SQL, execute it, and repair failed queries.

    Returns analysis fields plus data, success, attempts, and error.
    """
    if not question or not question.strip():
        raise ValueError("Please enter a business question.")
    if not frames:
        raise ValueError("No warehouse tables were loaded.")

    available_tables = list(frames.keys())
    schema = live_schema_description(connection, frames)
    analysis = generate_analysis(question, frames)
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

            explanation = str(analysis.get("explanation", "")).strip()
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
                repaired_sql = repaired.get("sql", "").strip()

                if not repaired_sql:
                    analysis.update(repaired)
                    last_error = repaired.get("explanation") or last_error
                    break

                sql = repaired_sql
                for key in (
                    "chart_type", "title", "explanation", "metric", "dimension"
                ):
                    if key in repaired:
                        analysis[key] = repaired[key]

            except Exception as repair_exc:
                last_error = f"{last_error}\nSQL repair failed: {repair_exc}"
                break

    return {
        **analysis,
        "sql": sql,
        "data": pd.DataFrame(),
        "success": False,
        "attempts": min(MAX_SQL_RETRIES + 1, attempt + 1),
        "error": (
            "Query failed or could not be repaired.\n"
            f"Last error: {last_error}\nGenerated SQL: {sql}"
        ),
    }
