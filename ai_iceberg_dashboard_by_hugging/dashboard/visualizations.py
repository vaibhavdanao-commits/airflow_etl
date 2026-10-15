from __future__ import annotations

import re
from typing import Optional

import pandas as pd
import plotly.express as px
import streamlit as st


# ============================================================
# CONFIGURATION
# ============================================================

CHART_LAYOUT = [
    ("Donut Chart", "donut"),
    ("Bar Chart", "bar"),
    ("Pie Chart", "pie"),
    ("Line Graph", "line"),
]


# ============================================================
# DATA HELPERS
# ============================================================

def _numeric_columns(df: pd.DataFrame) -> list[str]:
    return df.select_dtypes(include="number").columns.tolist()


def _is_time_column(column: str, series: pd.Series) -> bool:
    """Identify date/time columns using their names or data types."""
    if pd.api.types.is_datetime64_any_dtype(series):
        return True

    name = column.lower()
    time_words = (
        "date", "month", "year", "day", "timestamp",
        "order_date", "sale_date", "created_at",
    )

    if any(word in name for word in time_words):
        return True

    if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
        sample = series.dropna().astype(str).head(20)

        if sample.empty:
            return False

        # Recognize values such as 2026-01, 2026-01-15,
        # Jan 2026, and 2026-01-15 10:30:00.
        pattern = re.compile(
            r"^\d{4}[-/]\d{1,2}([-/]\d{1,2})?"
            r"|^[A-Za-z]{3,9}\s+\d{4}$"
        )

        matches = sample.map(lambda value: bool(pattern.search(value)))

        return matches.mean() >= 0.7

    return False


def _prepare_time_column(
    df: pd.DataFrame,
    column: str,
) -> tuple[pd.DataFrame, Optional[str]]:
    """Create a parsed date column when the values support it."""
    result = df.copy()

    if column not in result.columns:
        return result, None

    try:
        parsed = pd.to_datetime(result[column], errors="coerce")

        if parsed.notna().mean() >= 0.7:
            result[column] = parsed
            return result, column
    except (ValueError, TypeError):
        pass

    return result, None


def _category_columns(df: pd.DataFrame) -> list[str]:
    numeric = set(_numeric_columns(df))

    return [
        column
        for column in df.columns
        if column not in numeric
        and not _is_time_column(column, df[column])
    ]


def _time_columns(df: pd.DataFrame) -> list[str]:
    return [
        column
        for column in df.columns
        if _is_time_column(column, df[column])
    ]


def _get_measure(df: pd.DataFrame) -> Optional[str]:
    """Choose a sensible numeric measure for sales analytics."""
    numeric = _numeric_columns(df)

    if not numeric:
        return None

    preferred = [
        "sales_amount",
        "total_sales",
        "revenue",
        "total_revenue",
        "amount",
        "quantity",
        "order_count",
        "count",
    ]

    lower_map = {column.lower(): column for column in numeric}

    for candidate in preferred:
        if candidate in lower_map:
            return lower_map[candidate]

    return numeric[0]


def _get_dimension(
    df: pd.DataFrame,
    exclude: Optional[list[str]] = None,
) -> Optional[str]:
    exclude = exclude or []

    for column in _category_columns(df):
        if column not in exclude and df[column].nunique(dropna=True) > 1:
            return column

    return None


def _aggregate(
    df: pd.DataFrame,
    dimension: str,
    measure: str,
) -> pd.DataFrame:
    """Aggregate data safely for charting."""
    data = df[[dimension, measure]].copy()
    data[measure] = pd.to_numeric(data[measure], errors="coerce")
    data = data.dropna(subset=[dimension, measure])

    if data.empty:
        return data

    return (
        data.groupby(dimension, as_index=False, dropna=False)[measure]
        .sum()
        .sort_values(measure, ascending=False)
    )


# ============================================================
# CHART BUILDERS
# ============================================================

def _make_donut(
    df: pd.DataFrame,
    dimension: str,
    measure: str,
    title: str,
):
    data = _aggregate(df, dimension, measure)

    if data.empty:
        return None

    # Avoid overcrowded labels.
    if len(data) > 12:
        top = data.head(10).copy()
        other_value = data.iloc[10:][measure].sum()

        if other_value:
            top = pd.concat(
                [
                    top,
                    pd.DataFrame(
                        [{dimension: "Other", measure: other_value}]
                    ),
                ],
                ignore_index=True,
            )

        data = top

    return px.pie(
        data,
        names=dimension,
        values=measure,
        hole=0.55,
        title=title,
    )


def _make_bar(
    df: pd.DataFrame,
    dimension: str,
    measure: str,
    title: str,
):
    data = _aggregate(df, dimension, measure)

    if data.empty:
        return None

    # Use chronological order for date/time dimensions.
    if pd.api.types.is_datetime64_any_dtype(data[dimension]):
        data = data.sort_values(dimension)
    else:
        data = data.sort_values(measure, ascending=False).head(20)

    return px.bar(
        data,
        x=dimension,
        y=measure,
        title=title,
        labels={dimension: dimension.replace("_", " ").title()},
    )


def _make_pie(
    df: pd.DataFrame,
    dimension: str,
    measure: str,
    title: str,
):
    data = _aggregate(df, dimension, measure)

    if data.empty:
        return None

    # Limit slices to keep the chart readable.
    data = data.sort_values(measure, ascending=False).head(10)

    return px.pie(
        data,
        names=dimension,
        values=measure,
        title=title,
    )


def _make_line(
    df: pd.DataFrame,
    dimension: str,
    measure: str,
    title: str,
):
    data = _aggregate(df, dimension, measure)

    if data.empty:
        return None

    if pd.api.types.is_datetime64_any_dtype(data[dimension]):
        data = data.sort_values(dimension)
    else:
        data = data.head(30)

    return px.line(
        data,
        x=dimension,
        y=measure,
        markers=True,
        title=title,
        labels={dimension: dimension.replace("_", " ").title()},
    )


def _style_chart(fig) -> None:
    fig.update_layout(
        autosize=True,
        margin=dict(l=20, r=20, t=60, b=30),
        legend_title_text="",
    )


# ============================================================
# MAIN DASHBOARD
# ============================================================

def show_visualization(
    df: pd.DataFrame,
    question: str,
    suggested_type: str = "auto",
    key_prefix: str = "result",
) -> None:
    """
    Display four related charts for a single SQL query result.

    The function does not execute SQL or modify the warehouse.
    It only visualizes the DataFrame supplied by the dashboard.
    """

    if df is None or df.empty:
        st.info("The query returned no rows, so there is nothing to visualize.")
        return

    data = df.copy()
    data.columns = [str(column) for column in data.columns]

    numeric = _numeric_columns(data)

    if not numeric:
        st.warning(
            "This query returned no numeric columns. "
            "A numeric measure is needed for sales charts."
        )
        st.dataframe(data, use_container_width=True, hide_index=True)
        return

    measure = _get_measure(data)

    if measure is None:
        st.dataframe(data, use_container_width=True, hide_index=True)
        return

    # Identify a time dimension and regular business dimensions.
    time_columns = _time_columns(data)
    categories = _category_columns(data)

    time_dimension = time_columns[0] if time_columns else None
    category_dimension = _get_dimension(data)

    # Use the time field for bar/line charts when available.
    trend_dimension = time_dimension or category_dimension

    # Use a business category for donut/pie where possible.
    share_dimension = category_dimension or trend_dimension

    # If the result has just one numeric measure, we can still
    # create all four charts from the same available dimension.
    if share_dimension is None:
        st.warning(
            "The result contains only numeric measures and no category "
            "or date dimension. Please include a dimension in your SQL."
        )
        st.dataframe(data, use_container_width=True, hide_index=True)
        return

    # Parse recognized date strings for proper chronological sorting.
    if time_dimension:
        data, parsed_column = _prepare_time_column(data, time_dimension)
        if parsed_column:
            time_dimension = parsed_column
            trend_dimension = parsed_column

    if trend_dimension is None:
        trend_dimension = share_dimension

    # Dashboard heading and query context.
    st.subheader("📊 Multi-Chart Analytics Dashboard")
    st.caption(f"Business question: {question}")

    # Summary metrics.
    total_value = pd.to_numeric(data[measure], errors="coerce").sum()
    left, middle, right = st.columns(3)

    left.metric(
        f"Total {measure.replace('_', ' ').title()}",
        f"{total_value:,.2f}",
    )
    middle.metric("Rows returned", f"{len(data):,}")
    right.metric(
        "Categories",
        f"{data[share_dimension].nunique(dropna=True):,}",
    )

    st.divider()

    # Four visualizations in a 2x2 layout.
    row1_left, row1_right = st.columns(2)

    with row1_left:
        st.markdown("#### 1. Donut Chart")
        fig = _make_donut(
            data,
            share_dimension,
            measure,
            f"{measure.replace('_', ' ').title()} Share by "
            f"{share_dimension.replace('_', ' ').title()}",
        )

        if fig is not None:
            _style_chart(fig)
            st.plotly_chart(
                fig,
                use_container_width=True,
                key=f"{key_prefix}_donut",
            )
        else:
            st.info("Not enough valid data for the donut chart.")

    with row1_right:
        st.markdown("#### 2. Bar Chart")
        fig = _make_bar(
            data,
            trend_dimension,
            measure,
            f"{measure.replace('_', ' ').title()} Comparison",
        )

        if fig is not None:
            _style_chart(fig)
            st.plotly_chart(
                fig,
                use_container_width=True,
                key=f"{key_prefix}_bar",
            )
        else:
            st.info("Not enough valid data for the bar chart.")

    row2_left, row2_right = st.columns(2)

    with row2_left:
        st.markdown("#### 3. Pie Chart")
        fig = _make_pie(
            data,
            share_dimension,
            measure,
            f"{measure.replace('_', ' ').title()} Distribution",
        )

        if fig is not None:
            _style_chart(fig)
            st.plotly_chart(
                fig,
                use_container_width=True,
                key=f"{key_prefix}_pie",
            )
        else:
            st.info("Not enough valid data for the pie chart.")

    with row2_right:
        st.markdown("#### 4. Line Graph")
        fig = _make_line(
            data,
            trend_dimension,
            measure,
            f"{measure.replace('_', ' ').title()} Trend",
        )

        if fig is not None:
            _style_chart(fig)
            st.plotly_chart(
                fig,
                use_container_width=True,
                key=f"{key_prefix}_line",
            )
        else:
            st.info("Not enough valid data for the line graph.")

    # Show the SQL result data below the dashboard.
    with st.expander("View query result data"):
        st.dataframe(data, use_container_width=True, hide_index=True)
