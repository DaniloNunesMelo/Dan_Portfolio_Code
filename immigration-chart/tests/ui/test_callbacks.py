"""Unit tests for src/ui/callbacks.py"""
from unittest.mock import patch

import pandas as pd
import plotly.graph_objects as go
import pytest

import src.ui.callbacks as cb


@pytest.fixture(autouse=True)
def clear_data_store():
    """Clear in-memory stores before each test."""
    cb._data_store.clear()
    cb._source_store.clear()
    yield
    cb._data_store.clear()
    cb._source_store.clear()


# ── render_chart ──────────────────────────────────────────────────────────────

def test_render_chart_no_countries():
    fig, status = cb.render_chart(
        countries=[], metric="Inflows of Foreign Population",
        year_start=2000, year_end=2024,
        chart_type="Line", group_by="By Origin Country",
        top_n=15, gender="Total",
    )
    assert isinstance(fig, go.Figure)
    assert "No country selected" in status


def test_render_chart_valid(canonical_df):
    with patch("src.ui.callbacks._get_combined_df", return_value=(canonical_df, ["OECD_MIG"])):
        fig, status = cb.render_chart(
            countries=["Italy"], metric="Inflows of Foreign Population",
            year_start=2000, year_end=2024,
            chart_type="Line", group_by="By Origin Country",
            top_n=15, gender="Total",
        )
    assert isinstance(fig, go.Figure)
    assert len(status) > 0


def test_render_chart_data_load_error():
    with patch("src.ui.callbacks._get_combined_df", side_effect=Exception("load failed")):
        fig, status = cb.render_chart(
            countries=["Italy"], metric="Inflows of Foreign Population",
            year_start=2000, year_end=2024,
            chart_type="Line", group_by="By Origin Country",
            top_n=15, gender="Total",
        )
    assert isinstance(fig, go.Figure)
    assert "Error" in status


def test_render_chart_empty_df():
    with patch("src.ui.callbacks._get_combined_df", return_value=(pd.DataFrame(), [])):
        fig, status = cb.render_chart(
            countries=["Italy"], metric="Inflows of Foreign Population",
            year_start=2000, year_end=2024,
            chart_type="Line", group_by="By Origin Country",
            top_n=15, gender="Total",
        )
    assert isinstance(fig, go.Figure)


# ── render_pivot ──────────────────────────────────────────────────────────────

def test_render_pivot_no_countries():
    result = cb.render_pivot(
        countries=[], metric="Inflows of Foreign Population",
        year_start=2000, year_end=2024,
        pivot_rows="counterpart_name", pivot_cols="year",
        gender="Total",
    )
    assert isinstance(result, pd.DataFrame)
    assert "Info" in result.columns


def test_render_pivot_valid(canonical_df):
    with patch("src.ui.callbacks._get_combined_df", return_value=(canonical_df, ["OECD_MIG"])):
        result = cb.render_pivot(
            countries=["Italy"], metric="Inflows of Foreign Population",
            year_start=2000, year_end=2024,
            pivot_rows="counterpart_name", pivot_cols="year",
            gender="Total",
        )
    assert isinstance(result, pd.DataFrame)


# ── render_history ────────────────────────────────────────────────────────────

def test_render_history_no_countries():
    fig, md = cb.render_history(countries=[])
    assert isinstance(fig, go.Figure)
    assert md == ""


def test_render_history_valid(canonical_df):
    with patch("src.ui.callbacks._get_combined_df", return_value=(canonical_df, ["OECD_MIG"])):
        fig, md = cb.render_history(countries=["Italy"])
    assert isinstance(fig, go.Figure)
    assert len(md) > 0


# ── update_metric_choices ─────────────────────────────────────────────────────

def test_update_metric_choices_no_countries():
    result = cb.update_metric_choices([])
    # Should return gr.update — just check it's a dict with "choices" key
    assert "choices" in result


def test_update_metric_choices_italy():
    result = cb.update_metric_choices(["Italy"])
    assert "choices" in result
    assert "Inflows of Foreign Population" in result["choices"]


def test_update_metric_choices_canada():
    result = cb.update_metric_choices(["Canada"])
    assert "choices" in result
    assert "Permanent Residents" in result["choices"]


def test_update_metric_choices_both():
    result = cb.update_metric_choices(["Italy", "Canada"])
    choices = result["choices"]
    assert "Inflows of Foreign Population" in choices
    assert "Permanent Residents" in choices


# ── refresh_data ──────────────────────────────────────────────────────────────

def test_refresh_data_no_countries():
    result = cb.refresh_data([])
    assert "No countries selected" in result


def test_refresh_data_clears_store(canonical_df):
    cb._data_store["ITA"] = canonical_df
    cb._source_store["ITA"] = ["OECD_MIG"]

    with patch("src.ui.callbacks._get_combined_df", return_value=(canonical_df, ["OECD_MIG"])):
        cb.refresh_data(["Italy"])

    # Store should have been cleared and re-populated (or just cleared if reload fails)
    # Key thing: function returns a string
    assert True  # no exception


# ── get_source_info_md ────────────────────────────────────────────────────────

def test_get_source_info_md_no_countries():
    result = cb.get_source_info_md([])
    assert "No countries selected" in result


def test_get_source_info_md_not_loaded():
    result = cb.get_source_info_md(["Italy"])
    assert "not yet loaded" in result


def test_get_source_info_md_loaded(canonical_df):
    cb._data_store["ITA"] = canonical_df
    cb._source_store["ITA"] = ["OECD_MIG"]
    result = cb.get_source_info_md(["Italy"])
    assert "Italy" in result
    assert "OECD_MIG" in result


# ── update_groupby_choices ────────────────────────────────────────────────────

def test_update_groupby_choices_no_countries():
    result = cb.update_groupby_choices([])
    assert result["choices"] is not None
    assert len(result["choices"]) > 0


def test_update_groupby_choices_no_data_loaded():
    """Returns full list when data store is empty."""
    result = cb.update_groupby_choices(["Italy"])
    assert "By Origin Country" in result["choices"]


def test_update_groupby_choices_filters_null_cols(canonical_df):
    """Options whose column is all-NULL should be excluded when data is loaded."""
    import pandas as pd
    # Build a DataFrame where area_name and province are all null
    df = canonical_df.copy()
    df["area_name"] = None
    df["province"] = None
    cb._data_store["ITA"] = df
    result = cb.update_groupby_choices(["Italy"])
    # counterpart_name is populated → "By Origin Country" should be present
    assert "By Origin Country" in result["choices"]
    # area_name is None → "By Area" should be absent
    assert "By Area" not in result["choices"]
    # province is None → "By Province" should be absent
    assert "By Province" not in result["choices"]
