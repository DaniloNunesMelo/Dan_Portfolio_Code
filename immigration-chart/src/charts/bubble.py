import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from .registry import register, _top_n_groups


@register("Bubble")
def build_bubble(df: pd.DataFrame, group_col: str, top_n: int, title: str) -> go.Figure:
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
    agg = agg[agg["obs_value"] > 0]

    fig = px.scatter(
        agg, x="year", y="obs_value",
        size="obs_value",
        color=group_col if group_col in agg.columns else None,
        size_max=60,
        title=title,
        labels={"obs_value": "People", "year": "Year", group_col: ""},
    )
    fig.update_layout(
        legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    return fig
