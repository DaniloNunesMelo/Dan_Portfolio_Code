import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from .registry import register


@register("Choropleth")
def build_choropleth(df: pd.DataFrame, group_col: str, top_n: int, title: str) -> go.Figure:
    # Aggregate by origin country
    agg = (
        df.groupby(["counterpart", "counterpart_name"], dropna=False)["obs_value"]
        .sum()
        .reset_index()
    )
    # Filter out aggregate/unknown codes
    agg = agg[~agg["counterpart"].isin(["_T", "_O", "_R", "UNK", "TOT", ""])].copy()
    agg = agg[agg["obs_value"] > 0]

    if len(agg) == 0:
        from .registry import _empty_figure
        return _empty_figure("No country-level data for choropleth")

    fig = px.choropleth(
        agg,
        locations="counterpart",
        locationmode="ISO-3",
        color="obs_value",
        hover_name="counterpart_name",
        color_continuous_scale="Blues",
        title=title,
        labels={"obs_value": "People", "counterpart": "Country Code"},
    )
    fig.update_layout(
        geo=dict(showframe=False, showcoastlines=True, projection_type="natural earth"),
        paper_bgcolor="white",
    )
    return fig
