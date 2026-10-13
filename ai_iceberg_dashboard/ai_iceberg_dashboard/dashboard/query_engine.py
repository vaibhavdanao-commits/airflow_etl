
from __future__ import annotations

import json
import os
import re
from typing import Any

import duckdb
import pandas as pd
import sqlglot
from sqlglot import exp
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage

MAX_RESULT_ROWS = max(1, int(os.getenv("MAX_RESULT_ROWS", "500")))
MAX_SQL_RETRIES = max(0, int(os.getenv("MAX_SQL_RETRIES", "2")))

ALLOWED_CHARTS = {
    "auto", "bar", "line", "pie", "donut", "area", "scatter",
    "histogram", "box", "violin", "treemap", "sunburst",
    "heatmap", "funnel", "table", "kpi",
}



def make_llm() -> ChatOpenAI:
    """Connect to Ollama and request JSON-formatted responses."""
    return ChatOpenAI(
        model=os.getenv("OLLAMA_MODEL", "llama3.2"),
        base_url=os.getenv(
            "OLLAMA_BASE_URL",
            "http://localhost:11434/v1",
        ),
        api_key=os.getenv("OLLAMA_API_KEY", "ollama"),
        temperature=0,
        max_retries=1,
        timeout=float(os.getenv("LLM_TIMEOUT_SECONDS", "120")),
        model_kwargs={
            "response_format": {"type": "json_object"}
        },
    )


def extract_json(text: str) -> dict[str, Any]:
    """Parse JSON robustly and provide useful diagnostics."""
    if not text or not text.strip():
        raise ValueError(
            "Ollama returned an empty response. Check that the model "
            "is running and has enough memory to generate a response."
        )

    cleaned = text.strip()

    # Remove Markdown fences.
    cleaned = re.sub(
        r"^\s*```(?:json)?\s*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s*```\s*$", "", cleaned)

    # First try parsing the entire response.
    try:
        result = json.loads(cleaned)
        if isinstance(result, dict):
            return result
    except json.JSONDecodeError:
        pass

    # Next, find a JSON object inside surrounding text.
    decoder = json.JSONDecoder()

    for match in re.finditer(r"\{", cleaned):
        try:
            result, _ = decoder.raw_decode(cleaned[match.start():])
            if isinstance(result, dict) and "sql" in result:
                return result
        except json.JSONDecodeError:
            continue

    # Last fallback: accept a SQL code block or a plain SELECT query.
    # Metadata gets safe defaults; the SQL still passes validation.
    sql_match = re.search(
        r"```(?:sql)?\s*(.*?)```",
        cleaned,
        flags=re.IGNORECASE | re.DOTALL,
    )

    if sql_match:
        candidate = sql_match.group(1).strip()
        if re.match(r"^(SELECT|WITH)\b", candidate, re.IGNORECASE):
            return {
                "sql": candidate,
                "chart_type": "auto",
                "title": "Analysis Results",
                "explanation": "SQL extracted from the model response.",
                "metric": "",
                "dimension": "",
            }

    raise ValueError(
        "Ollama did not return a valid JSON object or recognizable SQL.\n"
        f"Response received (first 1000 characters):\n{cleaned[:1000]}\n\n"
        "Check the model name and JSON-format support."
    )


def schema_description(
    frames: dict[str, pd.DataFrame],
) -> str:
    """Describe tables, columns, data types, and sample values."""
    chunks = []

    for name, df in frames.items():
        chunks.append(
            f'\nTABLE "{name}" ({len(df):,} rows)'
        )

        for column in df.columns:
            series = df[column]
            details = (
                f'  "{column}" ({series.dtype}); '
                f"nulls={int(series.isna().sum()):,}"
            )

            if (
                pd.api.types.is_object_dtype(series)
                or isinstance(series.dtype, pd.CategoricalDtype)
            ):
                examples = (
                    series.dropna()
                    .astype(str)
                    .drop_duplicates()
                    .head(8)
                    .tolist()
                )
                if examples:
                    details += (
                        "; example values="
                        + json.dumps(
                            examples, ensure_ascii=False
                        )
                    )

            chunks.append(details)

        if not df.empty:
            sample = (
                df.head(3)
                .astype(object)
                .where(pd.notna(df.head(3)), None)
                .to_dict(orient="records")
            )
            chunks.append(
                "Sample rows: "
                + json.dumps(
                    sample,
                    default=str,
                    ensure_ascii=False,
                )
            )

    return "\n".join(chunks)


def extract_json(text: str) -> dict[str, Any]:
    """Extract a JSON object from the model response."""
    cleaned = text.strip()

    cleaned = re.sub(
        r"^\s*```(?:json)?\s*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s*```\s*$", "", cleaned)

    try:
        result = json.loads(cleaned)
        if isinstance(result, dict):
            return result
    except json.JSONDecodeError:
        pass

    # Safely locate a JSON object even if extra text surrounds it.
    decoder = json.JSONDecoder()

    for match in re.finditer(r"\{", cleaned):
        try:
            result, _ = decoder.raw_decode(cleaned[match.start():])
            if isinstance(result, dict):
                return result
        except json.JSONDecodeError:
            continue

    raise ValueError(
        "Ollama did not return valid JSON. "
        "Try again or check the configured model."
    )


def generate_analysis(
    question: str,
    frames: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    """Convert a natural-language business question into SQL."""
    if not question.strip():
        raise ValueError("Please enter a business question.")

    if not frames:
        raise ValueError("No warehouse tables were loaded.")

    schema = schema_description(frames)

    system_prompt = f"""
You are an expert retail analytics engineer and DuckDB SQL developer.

Convert the user's question into ONE accurate, read-only DuckDB query.

Return ONLY a JSON object with these keys:
sql, chart_type, title, explanation, metric, dimension.

AVAILABLE SCHEMA:
{schema}

SQL REQUIREMENTS:
1. Use only the exact table and column names shown above.
2. Generate only one SELECT query. CTEs are allowed.
3. Never invent columns, tables, dates, measures, or join keys.
4. Never use external tables, files, URLs, or table functions.
5. Use explicit JOIN conditions supported by the actual schema.
6. Avoid joins that duplicate fact rows.
7. Use meaningful output aliases for charts.
8. Preserve every filter specified by the user.
9. Use case-insensitive matching for text filters when appropriate.
10. Do not use SELECT * for analytical aggregation queries.
11. For top-N requests, use the requested N and sort by the requested metric.
12. Do not add arbitrary date filters or assume an unrequested time period.
13. Return sql="" if the question cannot be answered from the schema.

RETAIL BUSINESS RULES:
- Revenue: use an existing verified revenue column when available.
  Otherwise use SUM(quantity * unit_price) when both columns exist
  and their meaning supports that calculation.
- Order count: use COUNT(DISTINCT order_id) if order_id exists.
- Units sold: use SUM of the actual quantity column.
- Average order value: calculate supported revenue divided by distinct
  order count, protecting against division by zero.
- Product questions: filter the actual product ID or product name column.
- City questions: filter the actual city column.
- Monthly trends: use the actual date column and group by calendar year
  and month. Return rows ordered chronologically, not months as columns.
- For dimensional joins, use the actual foreign keys and dimension keys.
- If historical SCD2 dimension rows exist, avoid joins that multiply facts.
- Do not assume that a column called amount necessarily means revenue.

QUESTION EXAMPLES:
"Product_0094 revenue for every month":
Filter to Product_0094, calculate supported revenue, group by year and
month, and sort chronologically.

"Nagpur city monthly sales":
Filter the actual city field to Nagpur, group by year and month,
and calculate the supported sales measure.

"Top 10 products by revenue":
Group by product, aggregate revenue, sort descending, return 10 rows.

"How many orders were placed?":
Count distinct order IDs rather than fact rows.

CHART RULES:
chart_type must be one of:
auto, bar, line, pie, donut, area, scatter, histogram, box, violin,
treemap, sunburst, heatmap, funnel, table, kpi.

Use line for time trends, bar for category comparisons, pie/donut for
a small number of categories, heatmap for two-dimensional comparisons,
and table for detailed records.

The SQL dialect is DuckDB.
""".strip()

    response = make_llm().invoke([
        SystemMessage(content=system_prompt),
        HumanMessage(
            content=f"Business question: {question}\nReturn JSON only."
        ),
    ])

    content = response.content

    if isinstance(content, list):
        content = "".join(
            str(item.get("text", ""))
            if isinstance(item, dict)
            else str(item)
            for item in content
        )

    result = extract_json(str(content))

    result.setdefault("sql", "")
    result.setdefault("chart_type", "auto")
    result.setdefault("title", "Analysis Results")
    result.setdefault("explanation", "")
    result.setdefault("metric", "")
    result.setdefault("dimension", "")

    result["sql"] = str(result["sql"]).strip()
    result["chart_type"] = str(
        result["chart_type"]
    ).lower().strip()

    if result["chart_type"] not in ALLOWED_CHARTS:
        result["chart_type"] = "auto"

    if result["sql"]:
        result["sql"] = validate_sql(
            result["sql"],
            list(frames.keys()),
        )

    return result


def validate_sql(
    sql: str,
    available_tables: list[str],
) -> str:
    """Validate SQL syntax, table names, and read-only operations."""
    if not sql or not sql.strip():
        raise ValueError(
            "The AI did not generate SQL for this question. "
            "Review the explanation or rephrase the question."
        )

    try:
        statements = sqlglot.parse(sql, read="duckdb")
    except Exception as exc:
        raise ValueError(
            f"Generated SQL could not be parsed: {exc}"
        ) from exc

    if len(statements) != 1 or statements[0] is None:
        raise ValueError("Only one SQL statement is allowed.")

    tree = statements[0]

    if not isinstance(tree, (exp.Select, exp.Union)):
        raise ValueError("Only read-only SELECT queries are allowed.")

    forbidden = (
        exp.Insert,
        exp.Update,
        exp.Delete,
        exp.Drop,
        exp.Create,
        exp.Command,
        exp.Copy,
        exp.Pragma,
        exp.Attach,
        exp.Transaction,
        exp.Merge,
        exp.Into,
    )

    if any(tree.find(kind) for kind in forbidden):
        raise ValueError(
            "The generated SQL contains a prohibited operation."
        )

    # Block external and file-reading table functions.
    for node in tree.walk():
        if isinstance(node, exp.Func):
            function_name = node.sql_name().upper()
            if function_name in {
                "READ_CSV", "READ_CSV_AUTO",
                "READ_PARQUET", "READ_JSON",
                "READ_JSON_AUTO", "PARQUET_SCAN",
                "CSV_SCAN", "GLOB",
            }:
                raise ValueError(
                    f"External/file-reading function "
                    f"'{function_name}' is prohibited."
                )

    allowed = {table.lower() for table in available_tables}
    cte_names = {
        cte.alias_or_name.lower()
        for cte in tree.find_all(exp.CTE)
    }

    for table in tree.find_all(exp.Table):
        name = table.name.lower()

        if table.db or table.catalog:
            raise ValueError(
                "Qualified or external table references are prohibited. "
                "Use registered warehouse table names only."
            )

        if name not in allowed and name not in cte_names:
            raise ValueError(
                f"SQL references unknown table '{table.name}'. "
                f"Available tables: {', '.join(available_tables)}"
            )

    return tree.sql(dialect="duckdb")


def run_read_only_query(
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    available_tables: list[str],
) -> pd.DataFrame:
    """Execute a validated SQL query with a bounded result."""
    safe_sql = validate_sql(sql, available_tables)

    bounded_sql = (
        f"SELECT * FROM ({safe_sql}) AS dashboard_result "
        f"LIMIT {MAX_RESULT_ROWS}"
    )

    try:
        return connection.execute(bounded_sql).fetchdf()
    except Exception as exc:
        raise RuntimeError(
            f"SQL execution failed: {exc}\n\n"
            f"Generated SQL:\n{safe_sql}"
        ) from exc


def generate_and_run(
    question: str,
    frames: dict[str, pd.DataFrame],
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, Any]:
    """
    Generate SQL, execute it, and retry SQL errors using model feedback.
    """
    analysis = generate_analysis(question, frames)
    sql = analysis["sql"]

    if not sql:
        return {
            **analysis,
            "data": pd.DataFrame(),
            "success": False,
        }

    last_error = None

    for attempt in range(MAX_SQL_RETRIES + 1):
        try:
            result_df = run_read_only_query(
                connection,
                sql,
                list(frames.keys()),
            )

            if result_df.empty:
                analysis["explanation"] = (
                    str(analysis["explanation"])
                    + " The query returned no rows. "
                    "Check the requested filters and source data."
                ).strip()

            return {
                **analysis,
                "sql": sql,
                "data": result_df,
                "success": True,
            }

        except Exception as exc:
            last_error = str(exc)

            if attempt >= MAX_SQL_RETRIES:
                break

            repair_prompt = f"""
Repair this DuckDB SQL query.

Business question:
{question}

Actual schema:
{schema_description(frames)}

Previous SQL:
{sql}

Execution error:
{last_error}

Return only JSON with keys:
sql, chart_type, title, explanation, metric, dimension.

Rules:
- Fix the actual error.
- Preserve the original business question and all filters.
- Use only the provided tables and columns.
- Use one read-only SELECT query.
- Do not invent schema details.
""".strip()

            response = make_llm().invoke([
                SystemMessage(
                    content="You are an expert DuckDB SQL repair engineer."
                ),
                HumanMessage(content=repair_prompt),
            ])

            content = response.content

            if isinstance(content, list):
                content = "".join(
                    str(item.get("text", ""))
                    if isinstance(item, dict)
                    else str(item)
                    for item in content
                )

            repaired = extract_json(str(content))
            sql = validate_sql(
                str(repaired.get("sql", "")),
                list(frames.keys()),
            )

    raise RuntimeError(
        f"Query failed after {MAX_SQL_RETRIES + 1} attempts.\n"
        f"Last error: {last_error}\nGenerated SQL: {sql}"
    )
