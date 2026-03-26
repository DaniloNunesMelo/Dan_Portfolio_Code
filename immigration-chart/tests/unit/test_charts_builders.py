"""Unit tests for individual chart builder functions."""
import pandas as pd
import plotly.graph_objects as go
import pytest

# Import builders to trigger registration
import src.charts.line     # noqa: F401
import src.charts.bar      # noqa: F401
import src.charts.heatmap  # noqa: F401
import src.charts.choropleth  # noqa: F401
import src.charts.pie      # noqa: F401
import src.charts.bubble   # noqa: F401

from src.charts.line import build_line
from src.charts.bar import build_bar
from src.charts.heatmap import build_heatmap
from src.charts.pie import build_pie
from src.charts.bubble import build_bubble


GROUP_COL = "counterpart_name"
TOP_N = 10
TITLE = "Test Chart"


# ── line chart ────────────────────────────────────────────────────────────────

def test_line_chart_returns_figure(canonical_df):
    fig = build_line(canonical_df, GROUP_COL, TOP_N, TITLE)
    assert isinstance(fig, go.Figure)


def test_line_chart_missing_group_col(canonical_df):
    fig = build_line(canonical_df, "nonexistent_col", TOP_N, TITLE)
    assert isinstance(fig, go.Figure)


def test_line_chart_empty_df():
    empty = pd.DataFrame({"year": pd.Series([], dtype=int),
                          "obs_value": pd.Series([], dtype=float),
                          "counterpart_name": pd.Series([], dtype=str)})
    fig = build_line(empty, GROUP_COL, TOP_N, TITLE)
    assert isinstance(fig, go.Figure)


# ── bar chart ─────────────────────────────────────────────────────────────────

def test_bar_chart_returns_figure(canonical_df):
    fig = build_bar(canonical_df, GROUP_COL, TOP_N, TITLE)
    assert isinstance(fig, go.Figure)


def test_bar_chart_missing_group_col(canonical_df):
    fig = build_bar(canonical_df, "nonexistent_col", TOP_N, TITLE)
    assert isinstance(fig, go.Figure)


# ── heatmap ───────────────────────────────────────────────────────────────────

def test_heatmap_returns_figure(canonical_df):
    fig = build_heatmap(canonical_df, GROUP_COL, TOP_N, TITLE)
    assert isinstance(fig, go.Figure)


def test_heatmap_missing_group_col_fallback(canonical_df):
    """Heatmap falls back to counterpart_name when group col is missing."""
    fig = build_heatmap(canonical_df, "province", TOP_N, TITLE)
    # province is None in canonical_df, so should fall back
    assert isinstance(fig, go.Figure)


def test_heatmap_both_cols_missing():
    """Returns empty figure when both group_col and counterpart_name missing."""
    df = pd.DataFrame({"year": [2010], "obs_value": [100.0]})
    fig = build_heatmap(df, "nonexistent", TOP_N, TITLE)
    assert isinstance(fig, go.Figure)


# ── pie chart ─────────────────────────────────────────────────────────────────

def test_pie_returns_figure(canonical_df):
    from src.charts.pie import build_pie
    fig = build_pie(canonical_df, GROUP_COL, TOP_N, TITLE)
    assert isinstance(fig, go.Figure)


# ── bubble chart ──────────────────────────────────────────────────────────────

def test_bubble_returns_figure(canonical_df):
    from src.charts.bubble import build_bubble
    fig = build_bubble(canonical_df, GROUP_COL, TOP_N, TITLE)
    assert isinstance(fig, go.Figure)


# ── choropleth ────────────────────────────────────────────────────────────────

def test_choropleth_returns_figure(canonical_df):
    from src.charts.choropleth import build_choropleth
    fig = build_choropleth(canonical_df, GROUP_COL, TOP_N, TITLE)
    assert isinstance(fig, go.Figure)


def test_choropleth_no_valid_iso3_returns_figure():
    """Even with invalid ISO-3 codes, should return a figure (not crash)."""
    from src.charts.choropleth import build_choropleth
    df = pd.DataFrame({
        "year": [2010], "counterpart_name": ["Unknown"],
        "counterpart": ["XXX"], "obs_value": [100.0],
    })
    fig = build_choropleth(df, "counterpart_name", TOP_N, TITLE)
    assert isinstance(fig, go.Figure)
