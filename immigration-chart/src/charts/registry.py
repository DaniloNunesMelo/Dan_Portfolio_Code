"""
Chart registry: dispatch table mapping chart_type → builder function.
"""
from typing import Callable
import pandas as pd
import plotly.graph_objects as go

_registry: dict[str, Callable] = {}

GROUP_BY_MAP = {
    "By Origin Country": "counterpart_name",
    "By Area": "area_name",
    "By Region": "reg_name",
    "By Province": "province",
    "By Immigration Category": "imm_category",
    "By Gender": "gender",
    "By Source": "source_dataset",
}


def register(chart_type: str):
    """Decorator to register a chart builder function."""
    def decorator(fn: Callable) -> Callable:
        _registry[chart_type] = fn
        return fn
    return decorator


def build_chart(
    df: pd.DataFrame,
    chart_type: str,
    group_by: str,
    year_start: int,
    year_end: int,
    top_n: int = 15,
    gender: str = "Total",
    metric: str | None = None,
    title: str = "",
) -> go.Figure:
    """
    Filter df then dispatch to the registered chart builder.
    Falls back to line chart if no builder found.
    """
    if df is None or len(df) == 0:
        return _empty_figure("No data available")

    # Filter by year
    df_f = df.copy()
    df_f["year"] = pd.to_numeric(df_f["year"], errors="coerce")
    df_f = df_f[df_f["year"].between(year_start, year_end, inclusive="both")]

    # Filter by gender
    if gender and gender != "All":
        df_f = df_f[df_f["gender"] == gender]

    # Filter by metric
    if metric:
        df_f = df_f[df_f["metric"] == metric]

    if len(df_f) == 0:
        return _empty_figure(f"No data for {metric or 'selected metric'} ({year_start}–{year_end})")

    # Resolve group column; fall back to counterpart_name if column has no data
    group_col = GROUP_BY_MAP.get(group_by, "counterpart_name")
    if group_col != "counterpart_name" and (
        group_col not in df_f.columns or df_f[group_col].isna().all()
    ):
        group_col = "counterpart_name"

    # Auto-title
    if not title:
        countries = " & ".join(df_f["ref_area_name"].dropna().unique().tolist())
        title = f"{metric or 'Immigration'} — {countries} ({year_start}–{year_end})"

    builder = _registry.get(chart_type, _registry.get("Line"))
    if builder is None:
        return _empty_figure("No chart builder registered")

    try:
        return builder(df_f, group_col, top_n, title)
    except Exception as e:
        return _empty_figure(f"Chart error: {e}")


def _empty_figure(message: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper", yref="paper",
        x=0.5, y=0.5,
        showarrow=False,
        font=dict(size=16, color="gray"),
    )
    fig.update_layout(
        xaxis_visible=False, yaxis_visible=False,
        plot_bgcolor="white", paper_bgcolor="white",
    )
    return fig


def _top_n_groups(df: pd.DataFrame, group_col: str, n: int) -> list:
    """Return top N group values by total obs_value."""
    if group_col not in df.columns:
        return []
    totals = (
        df.groupby(group_col)["obs_value"]
        .sum()
        .nlargest(n)
    )
    return totals.index.tolist()
