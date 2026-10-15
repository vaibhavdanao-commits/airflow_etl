# AI-Driven Dimensional Warehouse Dashboard

A local Streamlit dashboard that accepts natural-language retail questions, uses Ollama through the OpenAI-compatible API (`langchain_openai.ChatOpenAI`) to generate SQL, validates that SQL, runs it in an in-memory DuckDB connection over data read from the existing Apache Iceberg tables, and renders interactive Plotly charts.

## Existing warehouse defaults

- Project root: `D:\airflow_etl`
- Catalog database: `D:\airflow_etl\Warehouse\dimensional_model_catalog.db`
- Warehouse path: `D:\airflow_etl\Warehouse\dimensional_model`
- Namespace: `star_schema`
- Expected tables include `fact_sales`, `dim_customer`, `dim_product`, `dim_store`, `dim_date`

The dashboard does not create a catalog, namespace, Iceberg table, or persistent DuckDB database. It only reads Iceberg tables and registers their data in DuckDB memory for SQL analysis.

## Files

```text
ai_iceberg_dashboard/
├── requirements.txt
├── run_dashboard.bat
├── README.md
└── dashboard/
    ├── app.py
    ├── iceberg_reader.py
    ├── query_engine.py
    ├── visualizations.py
    └── .env.example
```

## Setup on Windows

1. Install Python 3.11 or 3.12 (recommended for compatibility with native dependencies).
2. Install Ollama and start it. In PowerShell:

   ```powershell
   ollama pull llama3.2
   ollama list
   ```

3. Copy `dashboard\.env.example` to `dashboard\.env` and verify all paths.
4. From this project folder, create/activate a virtual environment and install dependencies:

   ```powershell
   py -3.12 -m venv .venv
   .\.venv\Scripts\Activate.ps1
   python -m pip install --upgrade pip
   pip install -r requirements.txt
   ```

5. Start the dashboard:

   ```powershell
   streamlit run dashboard\app.py
   ```

   Or double-click `run_dashboard.bat`.

## Charts

Auto, bar, line, pie, donut, area, scatter, histogram, box, violin, treemap, sunburst, heatmap, funnel, KPI, and table. Chart availability depends on the shape of the returned query data.

## How queries work

1. PyIceberg loads the tables in `star_schema` and scans them into memory.
2. The app exposes the actual table columns and small sample rows to the local LLM.
3. The LLM returns JSON containing SQL, a chart suggestion, a title, and an explanation.
4. SQLGlot parses the SQL. Only a single read-only SELECT-like statement referencing registered tables is allowed.
5. DuckDB executes the validated query and the app displays the result, a chart, and CSV download.

## Important notes

- This version loads Iceberg tables into memory. It is suitable for local practice and moderate datasets, but large warehouses may require predicate pushdown or a SQL engine with native Iceberg support.
- The result display is capped by `MAX_RESULT_ROWS` (default 500).
- LLM output is not guaranteed to be correct. Inspect the generated SQL and business metric definitions, especially revenue, returns, discounts, and date semantics.
- If PyIceberg fails with `ImportError: DLL load failed while importing pyroaring`, use a clean Python 3.11/3.12 virtual environment and reinstall compatible PyIceberg and `pyroaring` wheels. This app reports the failure rather than creating a replacement/sample database.
- Do not commit `dashboard/.env` if it contains environment-specific paths or secrets.
