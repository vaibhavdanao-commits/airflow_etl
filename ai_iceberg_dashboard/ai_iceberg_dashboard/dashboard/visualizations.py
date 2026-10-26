
from __future__ import annotations

import re
from typing import Optional

import pandas as pd
import plotly.express as px
import streamlit as st


# ============================================================
# CONFIGURATION
# ============================================================

MAX_BAR_CATEGORIES = 20
MAX_PIE_CATEGORIES = 8
MAX_SCATTER_POINTS = 3000

TIME_WORDS = (
    "date", "month", "year", "day", "timestamp",
    "order_date", "sale_date", "created_at", "period",
)

ID_WORDS = (
    "id", "_id", "key", "code", "zip", "postal_code",
)

MEASURE_PRIORITY = (
    "total_sales",
    "sales_amount",
    "total_revenue",
    "revenue",
    "sales",
    "total_amount",
    "amount",
    "sales_value",
    "total_quantity",
    "quantity",
    "order_count",
    "customer_count",
    "product_count",
    "count",
)

CHART_ALIASES = {
    "donut": "donut",
    "doughnut": "donut",
    "pie": "pie",
    "bar": "bar",
    "line": "line",
    "area": "area",
    "scatter": "scatter",
    "histogram": "histogram",
    "box": "box",
    "auto": "auto",
    "kpi": "kpi",
}


# ============================================================
# COLUMN DETECTION
# ============================================================

def _normalise_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).lower()).strip("_")


def _numeric_columns(df: pd.DataFrame) -> list[str]:
    return df.select_dtypes(include="number").columns.tolist()


def _is_identifier(column: str) -> bool:
    """Detect identifier-like columns that should not be summed."""
    name = _normalise_name(column)

    if name in {"id", "order_id", "customer_id", "product_id",
                "store_id", "transaction_id"}:
        return True

    return any(
        name.endswith(word) or name == word.lstrip("_")
        for word in ID_WORDS
    )


def _is_time_column(column: str, series: pd.Series) -> bool:
    name = _normalise_name(column)

    if pd.api.types.is_datetime64_any_dtype(series):
        return True

    if any(word in name for word in TIME_WORDS):
        return True

    if (
        pd.api.types.is_object_dtype(series)
        or pd.api.types.is_string_dtype(series)
    ):
        sample = series.dropna().astype(str).head(30)

        if sample.empty:
            return False

        pattern = re.compile(
            r"^\d{4}[-/]\d{1,2}([-/]\d{1,2})?"
            r"|^[A-Za-z]{3,9}\s+\d{4}$"
        )

        matches = sample.map(
            lambda value: bool(pattern.search(value.strip()))
        )

        return matches.mean() >= 0.7

    return False


def _parse_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert date-like string columns into datetime where appropriate."""
    result = df.copy()

    for column in result.columns:
        if not _is_time_column(column, result[column]):
            continue

        if pd.api.types.is_datetime64_any_dtype(result[column]):
            continue

        try:
            parsed = pd.to_datetime(result[column], errors="coerce")

            if parsed.notna().mean() >= 0.7:
                result[column] = parsed
        except (ValueError, TypeError):
            continue

    return result


def _time_columns(df: pd.DataFrame) -> list[str]:
    return [
        column
        for column in df.columns
        if _is_time_column(column, df[column])
    ]


def _category_columns(df: pd.DataFrame) -> list[str]:
    numeric = set(_numeric_columns(df))

    return [
        column
        for column in df.columns
        if column not in numeric
        and not _is_time_column(column, df[column])
        and df[column].nunique(dropna=True) > 1
    ]


def _get_measures(df: pd.DataFrame) -> list[str]:
    """Rank numeric columns, excluding identifiers."""
    candidates = [
        column
        for column in _numeric_columns(df)
        if not _is_identifier(column)
    ]

    priority = {
        name: index
        for index, name in enumerate(MEASURE_PRIORITY)
    }

    return sorted(
        candidates,
        key=lambda column: (
            priority.get(_normalise_name(column), 999),
            _normalise_name(column),
        ),
    )


def _get_dimension(
    df: pd.DataFrame,
    exclude: Optional[list[str]] = None,
) -> Optional[str]:
    exclude = exclude or []

    # Prefer date/time dimensions for chronological results.
    for column in _time_columns(df):
        if column not in exclude and df[column].nunique(dropna=True) > 1:
            return column

    # Then use categorical dimensions.
    for column in _category_columns(df):
        if column not in exclude:
            return column

    return None


def _display_name(name: str) -> str:
    return str(name).replace("_", " ").strip().title()


# ============================================================
# DATA PREPARATION
# ============================================================

def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data.columns = [str(column) for column in data.columns]

    # Make duplicate column names unique.
    counts: dict[str, int] = {}
    unique_columns = []

    for column in data.columns:
        counts[column] = counts.get(column, 0) + 1
        unique_columns.append(
            column if counts[column] == 1
            else f"{column}_{counts[column]}"
        )

    data.columns = unique_columns

    # Convert numeric-looking object columns when conversion is safe.
    for column in data.columns:
        if (
            pd.api.types.is_object_dtype(data[column])
            and not _is_time_column(column, data[column])
        ):
            converted = pd.to_numeric(data[column], errors="coerce")

            if len(data) and converted.notna().mean() >= 0.9:
                data[column] = converted

    return _parse_time_columns(data)


def _aggregate(
    df: pd.DataFrame,
    dimension: str,
    measure: str,
    aggregation: str = "sum",
) -> pd.DataFrame:
    data = df[[dimension, measure]].copy()

    data[measure] = pd.to_numeric(data[measure], errors="coerce")
    data = data.dropna(subset=[dimension, measure])

    if data.empty:
        return data

    if aggregation == "mean":
        grouped = data.groupby(
            dimension, as_index=False, dropna=False
        )[measure].mean()
    elif aggregation == "count":
        grouped = data.groupby(
            dimension, as_index=False, dropna=False
        )[measure].count()
    else:
        grouped = data.groupby(
            dimension, as_index=False, dropna=False
        )[measure].sum()

    if pd.api.types.is_datetime64_any_dtype(grouped[dimension]):
        return grouped.sort_values(dimension)

    return grouped.sort_values(measure, ascending=False)


def _is_time_series(df: pd.DataFrame, dimension: str) -> bool:
    return _is_time_column(dimension, df[dimension])


def _question_requests_share(question: str) -> bool:
    q = question.lower()

    return any(word in q for word in (
        "share", "distribution", "percentage", "proportion",
        "breakdown", "contribution", "split",
    ))


def _question_requests_trend(question: str) -> bool:
    q = question.lower()

    return any(word in q for word in (
        "trend", "over time", "monthly", "month-wise",
        "month wise", "daily", "weekly", "yearly",
        "each month", "every month", "per month",
        "by month", "by year", "by day", "growth",
        "historical",
    ))


def _question_requests_comparison(question: str) -> bool:
    q = question.lower()

    return any(word in q for word in (
        "compare", "comparison", "highest", "lowest",
        "top ", "ranking", "rank", "by city", "by product",
        "by category", "by store", "by payment",
        "versus", " vs ",
    ))


def _choose_chart_type(
    df: pd.DataFrame,
    question: str,
    dimension: Optional[str],
    measure: Optional[str],
    suggested_type: str = "auto",
) -> str:
    """
    Choose the chart most suitable for the query result.
    """
    requested = CHART_ALIASES.get(
        str(suggested_type).lower().strip(), "auto"
    )

    if requested != "auto":
        return requested

    if measure is None:
        return "kpi"

    if dimension is None:
        return "kpi"

    q = question.lower()

    # Time-based analysis should show chronology, not category share.
    if _is_time_series(df, dimension):
        if any(word in q for word in ("area chart", "cumulative", "volume")):
            return "area"
        return "line"

    # Explicit requests take priority.
    if any(word in q for word in ("donut", "doughnut")):
        return "donut"

    if "pie chart" in q:
        return "pie"

    if "bar chart" in q or "bar graph" in q:
        return "bar"

    if "scatter" in q or "correlation" in q or "relationship between" in q:
        if len(_get_measures(df)) >= 2:
            return "scatter"

    if "histogram" in q or "frequency distribution" in q:
        return "histogram"

    if "box plot" in q:
        return "box"

    # A share chart is useful only when category cardinality is small.
    category_count = df[dimension].nunique(dropna=True)

    if _question_requests_share(question) and category_count <= MAX_PIE_CATEGORIES:
        return "donut"

    # Trend words without a time field fall back to a comparison chart.
    if _question_requests_trend(question):
        return "bar"

    # Default for category comparisons.
    return "bar"


# ============================================================
# CHART BUILDERS
# ============================================================

def _make_chart(
    df: pd.DataFrame,
    chart_type: str,
    dimension: Optional[str],
    measure: Optional[str],
    question: str,
):
    if chart_type == "kpi" or measure is None:
        return None

    # Histogram needs one numeric column; no dimension is required.
    if chart_type == "histogram":
        numeric = _get_measures(df)

        if not numeric:
            return None

        return px.histogram(
            df,
            x=numeric[0],
            title=f"Distribution of {_display_name(numeric[0])}",
            labels={numeric[0]: _display_name(numeric[0])},
        )

    if dimension is None or dimension not in df.columns:
        return None

    # Scatter compares two numeric measures.
    if chart_type == "scatter":
        measures = _get_measures(df)

        if len(measures) < 2:
            return None

        x_measure = measures[0]
        y_measure = measures[1]

        data = df[[x_measure, y_measure]].copy()
        data[x_measure] = pd.to_numeric(data[x_measure], errors="coerce")
        data[y_measure] = pd.to_numeric(data[y_measure], errors="coerce")
        data = data.dropna()

        if len(data) > MAX_SCATTER_POINTS:
            data = data.sample(MAX_SCATTER_POINTS, random_state=42)

        if data.empty:
            return None

        return px.scatter(
            data,
            x=x_measure,
            y=y_measure,
            title=(
                f"{_display_name(y_measure)} vs "
                f"{_display_name(x_measure)}"
            ),
            labels={
                x_measure: _display_name(x_measure),
                y_measure: _display_name(y_measure),
            },
        )

    data = _aggregate(df, dimension, measure)

    if data.empty:
        return None

    is_time = _is_time_series(data, dimension)

    if is_time:
        data = data.sort_values(dimension)
    else:
        data = data.sort_values(measure, ascending=False)

    title = f"{_display_name(measure)} by {_display_name(dimension)}"

    labels = {
        dimension: _display_name(dimension),
        measure: _display_name(measure),
    }

    if chart_type in ("donut", "pie"):
        data = data.head(MAX_PIE_CATEGORIES)

        return px.pie(
            data,
            names=dimension,
            values=measure,
            hole=0.55 if chart_type == "donut" else 0,
            title=title,
            labels=labels,
        )

    if chart_type == "line":
        return px.line(
            data if is_time else data.head(MAX_BAR_CATEGORIES),
            x=dimension,
            y=measure,
            markers=True,
            title=title,
            labels=labels,
        )

    if chart_type == "area":
        return px.area(
            data if is_time else data.head(MAX_BAR_CATEGORIES),
            x=dimension,
            y=measure,
            title=title,
            labels=labels,
        )

    if chart_type == "box":
        return px.box(
            df,
            x=dimension,
            y=measure,
            title=title,
            labels=labels,
        )

    # Bar is the safe default for category comparisons.
    data = data.head(MAX_BAR_CATEGORIES)

    # Horizontal bars are easier to read for product and city names.
    return px.bar(
        data,
        x=measure,
        y=dimension,
        orientation="h",
        title=title,
        labels=labels,
    )


def _style_chart(fig) -> None:
    fig.update_layout(
        autosize=True,
        margin=dict(l=20, r=20, t=65, b=35),
        legend_title_text="",
        template="plotly_white",
    )

    if fig.data and getattr(fig.data[0], "orientation", None) == "h":
        fig.update_yaxes(autorange="reversed")


# ============================================================
# KPI DISPLAY
# ============================================================

def _show_kpis(
    df: pd.DataFrame,
    measures: list[str],
) -> None:
    if not measures:
        return

    # Prefer the most relevant measure, with up to 3 useful KPIs.
    chosen = measures[:3]
    columns = st.columns(len(chosen))

    for column, measure in zip(columns, chosen):
        values = pd.to_numeric(df[measure], errors="coerce").dropna()

        if values.empty:
            continue

        # A one-row SQL aggregate should display its actual value.
        if len(df) == 1:
            value = values.iloc[0]
        else:
            value = values.sum()

        column.metric(
            _display_name(measure),
            f"{value:,.2f}" if abs(value) < 1e15 else f"{value:.3e}",
        )


# ============================================================
# MAIN VISUALIZATION FUNCTION
# ============================================================

def show_visualization(
    df: pd.DataFrame,
    question: str,
    suggested_type: str = "auto",
    key_prefix: str = "result",
) -> None:
    """
    Automatically display one suitable chart and optionally one
    additional chart when it provides a different useful perspective.

    Compatible with:
        show_visualization(df, question)
        show_visualization(df, question, suggested_type="auto")
    """

    if df is None or df.empty:
        st.info("The query returned no rows, so there is nothing to visualize.")
        return

    data = _clean_data(df)

    if data.empty:
        st.info("No usable rows are available for visualization.")
        return

    measures = _get_measures(data)
    dimension = _get_dimension(data)

    st.subheader("📊 Query Visualization")
    st.caption(f"Business question: {question}")

    # Single aggregate result: KPI is usually the clearest presentation.
    if len(data) == 1:
        if measures:
            _show_kpis(data, measures)
            with st.expander("View query result data"):
                st.dataframe(data, use_container_width=True, hide_index=True)
            return

        st.dataframe(data, use_container_width=True, hide_index=True)
        st.info("This result has no numeric measures to chart.")
        return

    if not measures:
        st.info(
            "No numeric measure was found. Showing the query results as a table."
        )
        st.dataframe(data, use_container_width=True, hide_index=True)
        return

    if dimension is None:
        _show_kpis(data, measures)

        st.info(
            "This result has no usable category or date dimension. "
            "Showing its numeric summary instead of creating a misleading chart."
        )

        with st.expander("View query result data"):
            st.dataframe(data, use_container_width=True, hide_index=True)
        return

    measure = measures[0]

    # KPIs are useful context, but charts are limited to one or two.
    _show_kpis(data, measures)

    chart_type = _choose_chart_type(
        df=data,
        question=question,
        dimension=dimension,
        measure=measure,
        suggested_type=suggested_type,
    )

    # First chart: the best fit for this query.
    fig = _make_chart(
        data, chart_type, dimension, measure, question
    )

    if fig is None:
        st.dataframe(data, use_container_width=True, hide_index=True)
        st.info("A suitable chart could not be created for this result.")
        return

    _style_chart(fig)
    st.plotly_chart(
        fig,
        use_container_width=True,
        key=f"{key_prefix}_primary",
    )

    # Optional second chart: only when it adds a different perspective.
    # Do not duplicate a pie and donut or a line and area for one question.
    secondary_type = None

    if chart_type == "bar":
        if _question_requests_share(question):
            if data[dimension].nunique(dropna=True) <= MAX_PIE_CATEGORIES:
                secondary_type = "donut"

    elif chart_type in ("line", "area"):
        # For a time series, a bar chart may help compare individual periods.
        if len(data) <= MAX_BAR_CATEGORIES:
            secondary_type = "bar"

    elif chart_type in ("pie", "donut"):
        secondary_type = "bar"

    # Show a second chart only if the query has a useful alternative.
    if secondary_type:
        second_fig = _make_chart(
            data, secondary_type, dimension, measure, question
        )

        if second_fig is not None:
            st.markdown("#### Additional perspective")
            _style_chart(second_fig)
            st.plotly_chart(
                second_fig,
                use_container_width=True,
                key=f"{key_prefix}_secondary",
            )

    with st.expander("View query result data"):
        st.dataframe(data, use_container_width=True, hide_index=True)
