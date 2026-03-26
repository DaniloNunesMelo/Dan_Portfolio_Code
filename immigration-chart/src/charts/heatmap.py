import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from .registry import register, _top_n_groups


@register("Heatmap")
def build_heatmap(df: pd.DataFrame, group_col: str, top_n: int, title: str) -> go.Figure:
    top = _top_n_groups(df, group_col, top_n)
    if top and group_col in df.columns:
        df = df[df[group_col].isin(top)].copy()

    if group_col not in df.columns:
        group_col = "counterpart_name"
        if group_col not in df.columns:
            from .registry import _empty_figure
            return _empty_figure("Cannot build heatmap: group column missing")

    agg = (
        df.groupby([group_col, "year"], dropna=False)["obs_value"]
        .sum()
        .reset_index()
    )
    agg["year"] = agg["year"].astype(int)

    try:
        pivot = agg.pivot(index=group_col, columns="year", values="obs_value").fillna(0)
    except Exception:
        from .registry import _empty_figure
        return _empty_figure("Cannot pivot data for heatmap")

    fig = px.imshow(
        pivot,
        title=title,
        labels=dict(x="Year", y=group_col.replace("_", " ").title(), color="People"),
        aspect="auto",
        color_continuous_scale="Blues",
    )
    fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
    return fig
