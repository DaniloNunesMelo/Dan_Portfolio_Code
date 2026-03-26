import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from .registry import register, _top_n_groups


@register("Line")
def build_line(df: pd.DataFrame, group_col: str, top_n: int, title: str) -> go.Figure:
    top = _top_n_groups(df, group_col, top_n)
    if top and group_col in df.columns:
        df = df[df[group_col].isin(top)].copy()

    agg = (
        df.groupby(["year", group_col], dropna=False)["obs_value"]
        .sum()
        .reset_index()
    ) if group_col in df.columns else (
        df.groupby(["year"])["obs_value"].sum().reset_index()
    )
    agg["year"] = agg["year"].astype(int)

    fig = px.line(
        agg, x="year", y="obs_value",
        color=group_col if group_col in agg.columns else None,
        markers=True,
        title=title,
        labels={"obs_value": "People", "year": "Year", group_col: ""},
    )
    fig.update_layout(
        legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02),
        hovermode="x unified",
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    return fig
