
from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st


# ============================================================
# THEME
# ============================================================

CHART_COLORS = [
    "#635BDB",
    "#14B8A6",
    "#F59E0B",
    "#3B82F6",
    "#EC4899",
    "#8B5CF6",
    "#10B981",
    "#F97316",
    "#64748B",
    "#06B6D4",
]

BAR_COLOR = "#635BDB"
LINE_COLOR = "#14B8A6"

CHART_HEIGHT = 340

CHART_TYPES = [
    "Bar",
    "Line",
    "Donut",
    "Pie",
]


# ============================================================
# CHART SLOT MANAGEMENT
# ============================================================

def create_chart_slots(
    titles: list[str],
) -> list[Any]:

    slots = []

    for start in range(0, len(titles), 2):

        columns = st.columns(2, gap="large")

        for offset, column in enumerate(columns):

            index = start + offset

            if index >= len(titles):
                break

            with column:

                slot = st.empty()
                slots.append(slot)

                show_chart_loading(
                    slot,
                    titles[index],
                )

    return slots


def show_chart_loading(
    slot: Any,
    title: str,
) -> None:

    with slot.container(border=True):

        st.markdown(f"**{title}**")

        st.markdown(
            """
            <div style="
                height:230px;
                display:flex;
                flex-direction:column;
                align-items:center;
                justify-content:center;
                background:#F7F8FC;
                border:1px dashed #D8DEEA;
                border-radius:12px;
            ">
                <div style="font-size:28px;">⏳</div>
                <div style="
                    color:#53617A;
                    font-size:14px;
                    font-weight:600;
                ">
                    Generating visualization
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def show_chart_error(
    slot: Any,
    title: str,
    error: str,
) -> None:

    slot.empty()

    with slot.container(border=True):

        st.markdown(f"**{title}**")

        st.warning(
            "This visualization could not be generated."
        )

        with st.expander("View error details"):
            st.code(str(error))


# ============================================================
# DATA PREPARATION
# ============================================================

def _normalise_chart_type(
    chart_type: str,
) -> str:

    value = str(
        chart_type or "Bar"
    ).strip().lower()

    supported = {
        "bar": "Bar",
        "column": "Bar",
        "line": "Line",
        "pie": "Pie",
        "donut": "Donut",
    }

    return supported.get(value, "Bar")


def _prepare_chart_data(
    df: pd.DataFrame,
    dimension: str,
    measure: str,
    chart_type: str = "Bar",
    top_n: int = 10,
) -> pd.DataFrame:

    if df is None or df.empty:
        return pd.DataFrame()

    if dimension not in df.columns:
        return pd.DataFrame()

    if measure not in df.columns:
        return pd.DataFrame()

    working = df[
        [dimension, measure]
    ].copy()

    working[measure] = pd.to_numeric(
        working[measure],
        errors="coerce",
    )

    working = working.dropna(
        subset=[dimension, measure]
    )

    if working.empty:
        return pd.DataFrame()

    grouped = (
        working.groupby(
            dimension,
            dropna=False,
        )[measure]
        .sum()
        .reset_index()
    )

    if grouped.empty:
        return grouped

    chart_type = _normalise_chart_type(
        chart_type
    )

    # Chronological sorting for line charts.
    if chart_type == "Line":

        try:

            dates = pd.to_datetime(
                grouped[dimension],
                errors="coerce",
            )

            if dates.notna().all():

                grouped = (
                    grouped.assign(
                        _sort_value=dates
                    )
                    .sort_values("_sort_value")
                    .drop(columns="_sort_value")
                )

            else:

                grouped = grouped.sort_values(
                    dimension
                )

        except (TypeError, ValueError):

            grouped = grouped.sort_values(
                dimension
            )

    else:

        grouped = grouped.sort_values(
            measure,
            ascending=False,
        )

    if top_n and top_n > 0:

        grouped = grouped.head(top_n)

    return grouped.reset_index(drop=True)


# ============================================================
# CHART STYLING
# ============================================================

def _style_chart(
    fig: Any,
    title: str,
    chart_type: str,
) -> None:

    fig.update_layout(

        title=dict(
            text=title,
            font=dict(
                size=16,
                color="#172B52",
            ),
        ),

        height=CHART_HEIGHT,

        margin=dict(
            l=25,
            r=25,
            t=60,
            b=40,
        ),

        paper_bgcolor="white",
        plot_bgcolor="white",

        font=dict(
            family="Inter, sans-serif",
            color="#263653",
            size=12,
        ),

        hoverlabel=dict(
            bgcolor="white",
            font_size=12,
        ),

        showlegend=chart_type in ("Pie", "Donut"),

        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5,
        ),
    )

    if chart_type in ("Bar", "Line"):

        fig.update_xaxes(
            showgrid=True,
            gridcolor="#EDF0F6",
            zeroline=False,
        )

        fig.update_yaxes(
            showgrid=True,
            gridcolor="#EDF0F6",
            zeroline=False,
        )


# ============================================================
# BUILD PLOTLY CHART
# ============================================================

def build_chart(
    df: pd.DataFrame,
    dimension: str,
    measure: str,
    chart_type: str,
    title: str,
    top_n: int = 10,
) -> Any:

    chart_type = _normalise_chart_type(
        chart_type
    )

    chart_df = _prepare_chart_data(
        df=df,
        dimension=dimension,
        measure=measure,
        chart_type=chart_type,
        top_n=top_n,
    )

    if chart_df.empty:
        return None

    # --------------------------------------------------------
    # BAR CHART - SINGLE CONSISTENT COLOR
    # --------------------------------------------------------

    if chart_type == "Bar":

        chart_df = chart_df.sort_values(
            measure,
            ascending=True,
        )

        fig = px.bar(
            chart_df,
            x=measure,
            y=dimension,
            orientation="h",
            text_auto=".2s",
        )

        fig.update_traces(
            marker_color=BAR_COLOR,
            marker_line_width=0,
            textposition="outside",
            cliponaxis=False,
        )

    # --------------------------------------------------------
    # LINE CHART
    # --------------------------------------------------------

    elif chart_type == "Line":

        fig = px.line(
            chart_df,
            x=dimension,
            y=measure,
            markers=True,
        )

        fig.update_traces(
            line=dict(
                color=LINE_COLOR,
                width=3,
            ),
            marker=dict(
                size=8,
                color=LINE_COLOR,
            ),
        )

    # --------------------------------------------------------
    # DONUT CHART
    # --------------------------------------------------------

    elif chart_type == "Donut":

        fig = px.pie(
            chart_df,
            names=dimension,
            values=measure,
            hole=0.58,
            color_discrete_sequence=CHART_COLORS,
        )

        fig.update_traces(
            textposition="inside",
            textinfo="percent",
            marker=dict(
                line=dict(
                    color="white",
                    width=2,
                )
            ),
        )

    # --------------------------------------------------------
    # PIE CHART
    # --------------------------------------------------------

    else:

        fig = px.pie(
            chart_df,
            names=dimension,
            values=measure,
            hole=0,
            color_discrete_sequence=CHART_COLORS,
        )

        fig.update_traces(
            textposition="inside",
            textinfo="percent+label",
            marker=dict(
                line=dict(
                    color="white",
                    width=2,
                )
            ),
        )

    _style_chart(
        fig,
        title,
        chart_type,
    )

    return fig


# ============================================================
# FOUR CHARTS - AUTOMATIC 2 X 2 LAYOUT
# ============================================================

def show_analytics_view(
    slot: Any,
    view: dict[str, Any],
    show_data: bool = False,
) -> None:

    title = str(
        view.get(
            "title",
            "Analytics",
        )
    )

    df = view.get("data")

    dimension = view.get("dimension")
    measure = view.get("measure")

    top_n = int(
        view.get(
            "top_n",
            10,
        )
    )

    description = view.get(
        "description",
        "",
    )

    slot.empty()

    with slot.container():

        # ----------------------------------------------------
        # ANALYSIS HEADER
        # ----------------------------------------------------

        st.markdown(
            f"""
            <div style="
                padding:16px 20px;
                border-radius:12px;
                background:#F5F6FC;
                border-left:4px solid #635BDB;
                margin-bottom:15px;
            ">
                <div style="
                    font-size:19px;
                    font-weight:700;
                    color:#172B52;
                ">
                    {title}
                </div>
                <div style="
                    font-size:12px;
                    color:#64748B;
                    margin-top:5px;
                ">
                    Interactive analytics · Four visualization formats
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        if description:
            st.caption(str(description))

        # ----------------------------------------------------
        # DATA VALIDATION
        # ----------------------------------------------------

        if not isinstance(df, pd.DataFrame) or df.empty:

            st.info(
                "No records were found for this analysis."
            )

            return

        if not dimension or not measure:

            st.warning(
                "Missing dimension or measure."
            )

            return

        if (
            dimension not in df.columns
            or measure not in df.columns
        ):

            st.warning(
                "Required chart columns were not returned."
            )

            return

        # ----------------------------------------------------
        # PREPARE DATA ONCE FOR ALL FOUR CHARTS
        # ----------------------------------------------------

        chart_df = _prepare_chart_data(
            df=df,
            dimension=str(dimension),
            measure=str(measure),
            chart_type="Bar",
            top_n=top_n,
        )

        if chart_df.empty:

            st.info(
                "No suitable data is available."
            )

            return

        # ----------------------------------------------------
        # FIRST ROW: BAR + LINE
        # ----------------------------------------------------

        row_one = st.columns(
            2,
            gap="large",
        )

        for column, chart_type in zip(
            row_one,
            ["Bar", "Line"],
        ):

            with column:

                with st.container(border=True):

                    st.markdown(
                        f"**{chart_type} Chart**"
                    )

                    fig = build_chart(
                        df=chart_df,
                        dimension=str(dimension),
                        measure=str(measure),
                        chart_type=chart_type,
                        title=f"{chart_type} Analysis",
                        top_n=top_n,
                    )

                    if fig is not None:

                        st.plotly_chart(
                            fig,
                            use_container_width=True,
                            key=(
                                f"{view.get('chart_key', 'analytics')}"
                                f"_{chart_type.lower()}"
                            ),
                            config={
                                "displayModeBar": False,
                                "responsive": True,
                            },
                        )

        # ----------------------------------------------------
        # SECOND ROW: DONUT + PIE
        # ----------------------------------------------------

        row_two = st.columns(
            2,
            gap="large",
        )

        for column, chart_type in zip(
            row_two,
            ["Donut", "Pie"],
        ):

            with column:

                with st.container(border=True):

                    st.markdown(
                        f"**{chart_type} Chart**"
                    )

                    fig = build_chart(
                        df=chart_df,
                        dimension=str(dimension),
                        measure=str(measure),
                        chart_type=chart_type,
                        title=f"{chart_type} Analysis",
                        top_n=top_n,
                    )

                    if fig is not None:

                        st.plotly_chart(
                            fig,
                            use_container_width=True,
                            key=(
                                f"{view.get('chart_key', 'analytics')}"
                                f"_{chart_type.lower()}"
                            ),
                            config={
                                "displayModeBar": False,
                                "responsive": True,
                            },
                        )

        # ----------------------------------------------------
        # OPTIONAL CHART DATA
        # ----------------------------------------------------

        if show_data:

            with st.expander(
                "View analytical chart data"
            ):

                st.dataframe(
                    chart_df,
                    use_container_width=True,
                    hide_index=True,
                )


# ============================================================
# BACKWARD-COMPATIBLE FUNCTION
# ============================================================

def show_visualization(
    df: pd.DataFrame,
    question: str,
    suggested_type: str = "Bar",
    key_prefix: str = "result",
) -> None:

    if df is None or df.empty:

        st.info(
            "No data is available for visualization."
        )

        return

    numeric_columns = [
        column
        for column in df.select_dtypes(
            include="number"
        ).columns
        if not (
            str(column).lower().endswith("_id")
            or str(column).lower().endswith("_key")
        )
    ]

    dimensions = [
        column
        for column in df.columns
        if column not in numeric_columns
    ]

    if not numeric_columns or not dimensions:

        st.info(
            "The query result needs at least one dimension "
            "and one numeric measure."
        )

        return

    measure = numeric_columns[0]
    dimension = dimensions[0]

    view = {
        "title": question,
        "data": df,
        "dimension": dimension,
        "measure": measure,
        "chart_key": key_prefix,
    }

    show_analytics_view(
        st.empty(),
        view,
    )
