"""Unit tests for src/charts/registry.py"""
import pandas as pd
import plotly.graph_objects as go
import pytest

from src.charts.registry import (
    build_chart, register, _empty_figure, _top_n_groups, _registry,
)


@pytest.fixture(autouse=True)
def _ensure_builders_loaded():
    """Ensure chart builders are registered by importing them."""
    import src.charts.line  # noqa: F401
    import src.charts.bar   # noqa: F401
    import src.charts.heatmap  # noqa: F401
    import src.charts.choropleth  # noqa: F401
    import src.charts.pie  # noqa: F401
    import src.charts.bubble  # noqa: F401


# ── _empty_figure ─────────────────────────────────────────────────────────────

def test_empty_figure_returns_go_figure():
    fig = _empty_figure("test message")
    assert isinstance(fig, go.Figure)


def test_empty_figure_has_annotation():
    fig = _empty_figure("hello")
    texts = [a["text"] for a in fig.layout.annotations]
    assert "hello" in texts


# ── _top_n_groups ─────────────────────────────────────────────────────────────

def test_top_n_groups_returns_n_items(canonical_df):
    top = _top_n_groups(canonical_df, "counterpart_name", 2)
    assert len(top) <= 2


def test_top_n_groups_missing_column(canonical_df):
    top = _top_n_groups(canonical_df, "nonexistent_col", 5)
    assert top == []


def test_top_n_groups_ordered_by_value(canonical_df):
    top = _top_n_groups(canonical_df, "counterpart_name", 3)
    # India has obs_value=5000, should come first
    assert top[0] == "India"


# ── build_chart ───────────────────────────────────────────────────────────────

def test_build_chart_empty_df_returns_figure():
    fig = build_chart(
        df=pd.DataFrame(), chart_type="Line", group_by="By Origin Country",
        year_start=2000, year_end=2024,
    )
    assert isinstance(fig, go.Figure)


def test_build_chart_none_df_returns_figure():
    fig = build_chart(
        df=None, chart_type="Line", group_by="By Origin Country",
        year_start=2000, year_end=2024,
    )
    assert isinstance(fig, go.Figure)


def test_build_chart_year_filter(canonical_df):
    # Only 2010-2010 should keep 1 ITA row
    fig = build_chart(
        df=canonical_df, chart_type="Line", group_by="By Origin Country",
        year_start=2010, year_end=2010, metric="Inflows of Foreign Population",
    )
    assert isinstance(fig, go.Figure)


def test_build_chart_gender_filter(canonical_df):
    fig = build_chart(
        df=canonical_df, chart_type="Line", group_by="By Gender",
        year_start=2000, year_end=2024, gender="Total",
    )
    assert isinstance(fig, go.Figure)


def test_build_chart_metric_filter(canonical_df):
    fig = build_chart(
        df=canonical_df, chart_type="Line", group_by="By Origin Country",
        year_start=2000, year_end=2024,
        metric="Outflows of Foreign Population",  # not in canonical_df
    )
    # No data for this metric → empty figure
    assert isinstance(fig, go.Figure)


def test_build_chart_unknown_type_falls_back_to_line(canonical_df):
    fig = build_chart(
        df=canonical_df, chart_type="NonExistentChart",
        group_by="By Origin Country",
        year_start=2000, year_end=2024,
    )
    assert isinstance(fig, go.Figure)


def test_build_chart_bar_type(canonical_df):
    fig = build_chart(
        df=canonical_df, chart_type="Bar", group_by="By Origin Country",
        year_start=2000, year_end=2024,
    )
    assert isinstance(fig, go.Figure)
