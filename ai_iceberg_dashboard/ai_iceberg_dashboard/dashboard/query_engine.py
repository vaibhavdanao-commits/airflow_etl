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

MAX_RESULT_ROWS = int(os.getenv("MAX_RESULT_ROWS", "500"))


def make_llm() -> ChatOpenAI:
    return ChatOpenAI(
        model=os.getenv("OLLAMA_MODEL", "llama3.2"),
        base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1"),
        api_key=os.getenv("OLLAMA_API_KEY", "ollama"),
        temperature=0,
        max_retries=1,
        timeout=float(os.getenv("LLM_TIMEOUT_SECONDS", "90")),
    )


def schema_description(frames: dict[str, pd.DataFrame]) -> str:
    chunks = []
    for name, df in frames.items():
        cols = ", ".join(f'"{c}" ({str(df[c].dtype)})' for c in df.columns)
        chunks.append(f'TABLE "{name}" ({len(df):,} rows): {cols}')
        # Tiny samples help the model infer value formats without sending the whole dataset.
        if not df.empty:
            sample = df.head(2).where(pd.notna(df.head(2)), None).to_dict(orient="records")
            chunks.append(f"Sample rows for {name}: {json.dumps(sample, default=str, ensure_ascii=False)}")
    return "\n".join(chunks)


def extract_json(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", cleaned, flags=re.S)
        if not match:
            raise ValueError("The local AI did not return the expected JSON response. Try rephrasing the question.")
        return json.loads(match.group(0))


def generate_analysis(question: str, frames: dict[str, pd.DataFrame]) -> dict[str, Any]:
    llm = make_llm()
    schema = schema_description(frames)
    system_prompt = f"""You are a careful retail analytics engineer. Convert a user's business question into one DuckDB SELECT query against the registered in-memory tables.
Return ONLY valid JSON with keys: sql, chart_type, title, explanation, metric, dimension.
Rules:
- Only use tables and columns shown in the schema below. Table names are unqualified names.
- Only SELECT queries are allowed. No INSERT, UPDATE, DELETE, DROP, CREATE, ATTACH, COPY, PRAGMA, INSTALL, LOAD, or multiple statements.
- Do not invent columns, join keys, metrics, dates, or business facts.
- Join fact_sales to dimensions only when the real column names and relationships support it. Inspect schema names carefully.
- For revenue, use a revenue/amount column if one exists. Otherwise use quantity * unit_price only if both columns exist. If neither is possible, explain the limitation and use a supported metric.
- For monthly analysis, use a real date column; if the date dimension has year/month columns, use those when appropriate. Do not use arbitrary text as a date.
- If the user names a product or city, filter to that value when the column supports it. Use case-insensitive matching when appropriate.
- Avoid SELECT * for aggregate questions. Give clear aliases. Add sensible ORDER BY. For top-N requests, respect the requested N.
- Do not add LIMIT unless the user requests top-N or the output could be unbounded.
- chart_type must be one of: auto, bar, line, pie, donut, area, scatter, histogram, box, violin, treemap, sunburst, heatmap, funnel, table, kpi.
- Prefer line for time trends, bar for category comparisons/rankings, pie/donut for a small number of parts of a whole, scatter for two numeric variables, histogram for one numeric distribution, heatmap for two categorical/time dimensions, and table for detail queries.
- If the question cannot be answered from the schema, return sql as an empty string and explain why.
- The SQL dialect is DuckDB.

ACTUAL TABLE SCHEMA AND SAMPLE VALUES:
{schema}
"""
    response = llm.invoke([
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"Business question: {question}\nReturn JSON only."),
    ])
    content = response.content
    if isinstance(content, list):
        content = "".join(str(item.get("text", item)) if isinstance(item, dict) else str(item) for item in content)
    result = extract_json(str(content))
    result.setdefault("sql", "")
    result.setdefault("chart_type", "auto")
    result.setdefault("title", "Analysis results")
    result.setdefault("explanation", "")
    result.setdefault("metric", "")
    result.setdefault("dimension", "")
    return result


def validate_sql(sql: str, available_tables: list[str]) -> str:
    if not sql or not sql.strip():
        raise ValueError("AI did not produce a query for this question. Review the explanation or try a more specific question.")
    if ";" in sql.strip().rstrip(";"):
        raise ValueError("Multiple SQL statements are not allowed.")
    try:
        statements = sqlglot.parse(sql, read="duckdb")
    except Exception as exc:
        raise ValueError(f"Generated SQL could not be parsed: {exc}") from exc
    if len(statements) != 1 or statements[0] is None:
        raise ValueError("Only one SQL statement is allowed.")
    tree = statements[0]
    if not isinstance(tree, (exp.Select, exp.Subquery, exp.Union)):
        raise ValueError("Only read-only SELECT queries are allowed.")
    forbidden = (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create, exp.Command, exp.Copy, exp.Pragma, exp.Attach, exp.Transaction)
    if any(tree.find(kind) for kind in forbidden):
        raise ValueError("The generated query contains a prohibited operation.")
    allowed = {t.lower() for t in available_tables}
    cte_names = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    for table in tree.find_all(exp.Table):
        name = table.name.lower()
        if name not in allowed and name not in cte_names:
            raise ValueError(f"SQL references unknown table '{table.name}'. Available tables: {', '.join(available_tables)}")
        if table.db or table.catalog:
            raise ValueError("Qualified/external table references are not allowed. Use only the registered warehouse table names.")
    return tree.sql(dialect="duckdb")


def run_read_only_query(connection: duckdb.DuckDBPyConnection, sql: str, available_tables: list[str]) -> pd.DataFrame:
    safe_sql = validate_sql(sql, available_tables)
    # Wrapping the SELECT bounds output rows without changing the registered source tables.
    bounded_sql = f"SELECT * FROM ({safe_sql}) AS dashboard_result LIMIT {MAX_RESULT_ROWS}"
    try:
        return connection.execute(bounded_sql).fetchdf()
    except Exception as exc:
        raise RuntimeError(f"SQL execution failed: {exc}\n\nGenerated SQL:\n{safe_sql}") from exc
