from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

CHARTS = ["Auto", "Bar", "Line", "Pie", "Donut", "Area", "Scatter", "Histogram", "Box", "Violin", "Treemap", "Sunburst", "Heatmap", "Funnel", "Table"]


def _numeric_columns(df: pd.DataFrame) -> list[str]:
    return df.select_dtypes(include="number").columns.tolist()


def _category_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c not in _numeric_columns(df) and not pd.api.types.is_datetime64_any_dtype(df[c])]


def _choose_auto(df: pd.DataFrame, question: str) -> str:
    q = question.lower()
    if len(df.columns) == 1 and _numeric_columns(df):
        return "KPI"
    if any(x in q for x in ["trend", "monthly", "daily", "over time", "yearly", "by month"]):
        return "Line"
    if any(x in q for x in ["percentage", "share", "distribution", "proportion", "breakdown"]):
        return "Donut" if len(df) <= 8 else "Bar"
    if len(_numeric_columns(df)) == 1 and len(_category_columns(df)) >= 1:
        return "Bar"
    return "Table"


def show_visualization(df: pd.DataFrame, question: str, suggested_type: str = "auto", key_prefix: str = "result") -> None:
    if df.empty:
        st.info("The query returned no rows, so there is nothing to chart.")
        return

    default = suggested_type.title() if suggested_type.lower() != "auto" else _choose_auto(df, question)
    if default not in CHARTS and default != "KPI":
        default = "Table"
    options = CHARTS + ["KPI"]
    selected = st.selectbox(
        "Visualization",
        options,
        index=options.index(default) if default in options else 0,
        key=f"{key_prefix}_chart_type",
    )

    numeric = _numeric_columns(df)
    categories = _category_columns(df)
    cols = list(df.columns)
    if selected == "Table":
        st.dataframe(df, use_container_width=True, hide_index=True)
        return
    if selected == "KPI":
        if numeric:
            metric = st.selectbox("KPI metric", numeric, key=f"{key_prefix}_kpi_metric")
            total = pd.to_numeric(df[metric], errors="coerce").sum()
            st.metric(metric.replace("_", " ").title(), f"{total:,.2f}")
        else:
            st.metric("Rows returned", f"{len(df):,}")
        return

    if selected in ["Pie", "Donut", "Treemap", "Sunburst", "Funnel"]:
        if not categories or not numeric:
            st.warning(f"{selected} requires at least one category column and one numeric column. Choose Bar or Table instead.")
            st.dataframe(df, use_container_width=True, hide_index=True)
            return
        category = st.selectbox("Category", categories, key=f"{key_prefix}_cat")
        value = st.selectbox("Value", numeric, key=f"{key_prefix}_value")
        if selected in ["Pie", "Donut"]:
            fig = px.pie(df, names=category, values=value, hole=0.48 if selected == "Donut" else 0, title=selected)
        elif selected == "Treemap":
            fig = px.treemap(df, path=[category], values=value, title=selected)
        elif selected == "Sunburst":
            fig = px.sunburst(df, path=[category], values=value, title=selected)
        else:
            fig = px.funnel(df, x=value, y=category, title=selected)
    elif selected in ["Bar", "Line", "Area", "Scatter", "Box", "Violin", "Heatmap"]:
        if not numeric:
            st.warning(f"{selected} requires numeric data. Choose Table instead.")
            st.dataframe(df, use_container_width=True, hide_index=True)
            return
        x_default = categories[0] if categories else (cols[0] if cols else None)
        x = st.selectbox("X axis", cols, index=cols.index(x_default) if x_default in cols else 0, key=f"{key_prefix}_x")
        y = st.selectbox("Y axis / measure", numeric, key=f"{key_prefix}_y")
        color_options = ["(none)"] + [c for c in categories if c != x]
        color = st.selectbox("Color grouping", color_options, key=f"{key_prefix}_color")
        color_arg = None if color == "(none)" else color
        if selected == "Bar": fig = px.bar(df, x=x, y=y, color=color_arg, title=question)
        elif selected == "Line": fig = px.line(df, x=x, y=y, color=color_arg, markers=True, title=question)
        elif selected == "Area": fig = px.area(df, x=x, y=y, color=color_arg, title=question)
        elif selected == "Scatter": fig = px.scatter(df, x=x, y=y, color=color_arg, title=question)
        elif selected == "Box": fig = px.box(df, x=x, y=y, color=color_arg, title=question)
        elif selected == "Violin": fig = px.violin(df, x=x, y=y, color=color_arg, box=True, points="outliers", title=question)
        else:
            if len(numeric) < 2:
                st.warning("Heatmap requires two numeric columns or a pivoted result. Choose another chart.")
                st.dataframe(df, use_container_width=True, hide_index=True)
                return
            y2 = st.selectbox("Second numeric field", [c for c in numeric if c != y], key=f"{key_prefix}_heat_y")
            fig = px.density_heatmap(df, x=y, y=y2, title=question)
    elif selected == "Histogram":
        if not numeric:
            st.warning("Histogram requires a numeric column.")
            st.dataframe(df, use_container_width=True, hide_index=True)
            return
        value = st.selectbox("Numeric field", numeric, key=f"{key_prefix}_hist")
        fig = px.histogram(df, x=value, title=question)
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)
        return

    fig.update_layout(autosize=True, margin=dict(l=20, r=20, t=60, b=20), legend_title_text="")
    st.plotly_chart(fig, use_container_width=True)
