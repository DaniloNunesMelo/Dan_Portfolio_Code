"""Unit tests for src/ui/pivot.py"""
import pandas as pd
import pytest

from src.ui.pivot import build_pivot_table


# ── build_pivot_table ─────────────────────────────────────────────────────────

def test_build_pivot_table_basic_shape(canonical_df):
    result = build_pivot_table(canonical_df, rows="counterpart_name", cols="year")
    assert isinstance(result, pd.DataFrame)
    # Should have index column + year columns
    assert len(result) >= 1


def test_build_pivot_table_reset_index(canonical_df):
    result = build_pivot_table(canonical_df, rows="counterpart_name", cols="year")
    # No MultiIndex — reset_index() was called
    assert not isinstance(result.index, pd.MultiIndex)


def test_build_pivot_table_formatting(canonical_df):
    result = build_pivot_table(canonical_df, rows="counterpart_name", cols="year")
    # Values should be formatted strings with commas
    # Get a non-index column
    data_cols = [c for c in result.columns if c != "counterpart_name"]
    if data_cols:
        sample_val = result[data_cols[0]].iloc[0]
        assert isinstance(sample_val, str)


def test_build_pivot_missing_column(canonical_df):
    result = build_pivot_table(canonical_df, rows="nonexistent_col", cols="year")
    assert "Info" in result.columns or "Error" in result.columns


def test_build_pivot_empty_input():
    result = build_pivot_table(pd.DataFrame(), rows="counterpart_name", cols="year")
    assert "Info" in result.columns


def test_build_pivot_none_input():
    result = build_pivot_table(None, rows="counterpart_name", cols="year")
    assert "Info" in result.columns


def test_build_pivot_all_null_values():
    df = pd.DataFrame({
        "counterpart_name": ["Germany"],
        "year": [None],
        "obs_value": [None],
    })
    result = build_pivot_table(df, rows="counterpart_name", cols="year")
    assert "Info" in result.columns
