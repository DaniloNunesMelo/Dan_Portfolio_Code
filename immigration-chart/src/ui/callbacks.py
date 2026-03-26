"""
Gradio event handler callbacks.
Wires UI state → data loading → chart/pivot rendering.
"""
from __future__ import annotations

import traceback

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from ..processors.italy import load_italy
from ..processors.canada import load_canada
from ..processors.merge import merge_sources, get_coverage_info
from ..charts.registry import build_chart, _empty_figure
from .controls import (
    COUNTRY_TO_ISO, METRICS_BY_COUNTRY, COMMON_METRICS,
    DEFAULT_YEAR_START, DEFAULT_YEAR_END,
)
from .pivot import build_pivot_table

# In-memory data store: ISO3 → DataFrame
_data_store: dict[str, pd.DataFrame] = {}
_source_store: dict[str, list[str]] = {}


def _load_country(iso3: str) -> tuple[pd.DataFrame, list[str]]:
    """Load data for a single country, using the store as cache."""
    if iso3 in _data_store:
        return _data_store[iso3], _source_store.get(iso3, [])
    if iso3 == "ITA":
        df, sources = load_italy()
    elif iso3 == "CAN":
        df, sources = load_canada()
    else:
        return pd.DataFrame(), []
    _data_store[iso3] = df
    _source_store[iso3] = sources
    return df, sources


def _get_combined_df(countries: list[str]) -> tuple[pd.DataFrame, list[str]]:
    """Get merged DataFrame for selected countries."""
    if not countries:
        return pd.DataFrame(), []
    frames = []
    all_sources = []
    for country in countries:
        iso3 = COUNTRY_TO_ISO.get(country)
        if iso3:
            try:
                df, sources = _load_country(iso3)
                frames.append(df)
                all_sources.extend(sources)
            except Exception as e:
                print(f"[callbacks] Failed to load {country}: {e}")
    if not frames:
        return pd.DataFrame(), all_sources
    combined = merge_sources(frames)
    return combined, all_sources


def update_metric_choices(countries: list[str]) -> dict:
    """Return gr.update for metric dropdown based on selected countries."""
    import gradio as gr
    if not countries:
        return gr.update(choices=COMMON_METRICS, value=COMMON_METRICS[0])

    all_metrics = set()
    for c in countries:
        all_metrics.update(METRICS_BY_COUNTRY.get(c, []))

    # Common metrics first, then country-specific
    ordered = COMMON_METRICS.copy()
    for m in all_metrics:
        if m not in ordered:
            ordered.append(m)

    value = ordered[0] if ordered else None
    return gr.update(choices=ordered, value=value)


def render_chart(
    countries: list[str],
    metric: str,
    year_start: int,
    year_end: int,
    chart_type: str,
    group_by: str,
    top_n: int,
    gender: str,
) -> tuple[go.Figure, str]:
    """Main chart rendering callback."""
    if not countries:
        return _empty_figure("Select at least one country"), "No country selected"

    try:
        df, sources = _get_combined_df(countries)
        if df is None or len(df) == 0:
            return _empty_figure("No data loaded"), "Data load failed"

        fig = build_chart(
            df=df,
            chart_type=chart_type,
            group_by=group_by,
            year_start=int(year_start),
            year_end=int(year_end),
            top_n=int(top_n),
            gender=gender,
            metric=metric,
        )
        status = f"Loaded: {', '.join(sources)} | Rows: {len(df):,}"
        return fig, status

    except Exception as e:
        traceback.print_exc()
        return _empty_figure(f"Error: {e}"), f"Error: {e}"


def render_pivot(
    countries: list[str],
    metric: str,
    year_start: int,
    year_end: int,
    pivot_rows: str,
    pivot_cols: str,
    gender: str,
) -> pd.DataFrame:
    """Pivot table rendering callback."""
    if not countries:
        return pd.DataFrame({"Info": ["Select at least one country"]})
    try:
        df, _ = _get_combined_df(countries)
        if df is None or len(df) == 0:
            return pd.DataFrame({"Info": ["No data available"]})

        # Apply filters
        df_f = df.copy()
        df_f["year"] = pd.to_numeric(df_f["year"], errors="coerce")
        df_f = df_f[df_f["year"].between(int(year_start), int(year_end), inclusive="both")]
        if gender and gender != "All":
            df_f = df_f[df_f["gender"] == gender]
        if metric:
            df_f = df_f[df_f["metric"] == metric]

        return build_pivot_table(df_f, rows=pivot_rows, cols=pivot_cols)
    except Exception as e:
        traceback.print_exc()
        return pd.DataFrame({"Error": [str(e)]})


def render_history(countries: list[str]) -> tuple[go.Figure, str]:
    """History tab: coverage Gantt chart + markdown info table."""
    if not countries:
        return _empty_figure("Select at least one country"), ""
    try:
        df, sources = _get_combined_df(countries)
        if df is None or len(df) == 0:
            return _empty_figure("No data available"), ""

        coverage = get_coverage_info(df)
        if len(coverage) == 0:
            return _empty_figure("No coverage info"), ""

        # Build horizontal bar (Gantt-style) chart
        colors = px.colors.qualitative.Set2
        fig = go.Figure()
        for i, row in coverage.iterrows():
            color = colors[i % len(colors)]
            label = f"{row['source_dataset']} ({row['ref_area']})"
            if pd.notna(row["min_year"]) and pd.notna(row["max_year"]):
                fig.add_trace(go.Bar(
                    name=label,
                    x=[row["max_year"] - row["min_year"] + 1],
                    y=[label],
                    base=[row["min_year"]],
                    orientation="h",
                    marker_color=color,
                    text=f"{int(row['min_year'])}–{int(row['max_year'])} ({int(row['row_count']):,} rows)",
                    textposition="inside",
                    insidetextanchor="middle",
                    hovertemplate=(
                        f"<b>{label}</b><br>"
                        f"Years: {int(row['min_year'])}–{int(row['max_year'])}<br>"
                        f"Rows: {int(row['row_count']):,}<extra></extra>"
                    ),
                ))

        fig.update_layout(
            title="Data Coverage by Source",
            xaxis_title="Year",
            barmode="overlay",
            showlegend=False,
            plot_bgcolor="white",
            paper_bgcolor="white",
            height=max(300, len(coverage) * 60),
        )

        # Markdown table
        md_lines = ["**Data Sources Summary**\n",
                    "| Source | Country | Min Year | Max Year | Rows |",
                    "|--------|---------|----------|----------|------|"]
        for _, row in coverage.iterrows():
            md_lines.append(
                f"| {row['source_dataset']} | {row['ref_area']} "
                f"| {int(row['min_year']) if pd.notna(row['min_year']) else '?'} "
                f"| {int(row['max_year']) if pd.notna(row['max_year']) else '?'} "
                f"| {int(row['row_count']):,} |"
            )
        info_md = "\n".join(md_lines)
        return fig, info_md

    except Exception as e:
        traceback.print_exc()
        return _empty_figure(f"History error: {e}"), ""


def refresh_data(countries: list[str]) -> str:
    """Clear in-memory store for selected countries and reload."""
    if not countries:
        return "No countries selected"
    for country in countries:
        iso3 = COUNTRY_TO_ISO.get(country)
        if iso3 and iso3 in _data_store:
            del _data_store[iso3]
            _source_store.pop(iso3, None)
    try:
        df, sources = _get_combined_df(countries)
        return f"Refreshed: {', '.join(sources)} | Rows: {len(df):,}"
    except Exception as e:
        return f"Refresh failed: {e}"


def get_source_info_md(countries: list[str]) -> str:
    """Return markdown string describing loaded data sources."""
    if not countries:
        return "No countries selected."
    lines = ["**Loaded Data Sources**\n"]
    for country in countries:
        iso3 = COUNTRY_TO_ISO.get(country)
        if iso3 and iso3 in _data_store:
            df = _data_store[iso3]
            sources = _source_store.get(iso3, [])
            lines.append(f"### {country} (`{iso3}`)")
            lines.append(f"- **Sources**: {', '.join(sources)}")
            lines.append(f"- **Total rows**: {len(df):,}")
            yr = df["year"].dropna()
            if len(yr):
                lines.append(f"- **Year range**: {int(yr.min())} – {int(yr.max())}")
            lines.append(f"- **Metrics**: {', '.join(df['metric'].dropna().unique().tolist())}")
        else:
            lines.append(f"### {country} — not yet loaded")
    return "\n".join(lines)
