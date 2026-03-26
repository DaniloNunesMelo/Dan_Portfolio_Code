import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from .registry import register, _top_n_groups


@register("Pie")
def build_pie(df: pd.DataFrame, group_col: str, top_n: int, title: str) -> go.Figure:
    if group_col not in df.columns:
        group_col = "counterpart_name"

    agg = (
        df.groupby(group_col, dropna=False)["obs_value"]
        .sum()
        .nlargest(top_n)
        .reset_index()
    )
    agg = agg[agg["obs_value"] > 0]

    if len(agg) == 0:
        from .registry import _empty_figure
        return _empty_figure("No data for pie chart")

    fig = px.pie(
        agg, names=group_col, values="obs_value",
        title=title,
        labels={"obs_value": "People", group_col: ""},
        hole=0.3,
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(paper_bgcolor="white")
    return fig
