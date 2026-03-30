"""Integration tests for src/processors/italy.py"""
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from src.fetchers.base import FetchError
from src.processors.italy import load_italy

CANONICAL_COLUMNS = [
    "ref_area", "ref_area_name", "counterpart", "counterpart_name",
    "time_period", "year", "quarter", "var_code", "metric",
    "sex", "gender", "area_name", "reg_name", "province",
    "imm_category", "obs_value", "obs_status", "source_dataset", "fetch_ts",
]


@pytest.fixture
def mock_italy_fetchers(canonical_df):
    """Patch all live fetchers for Italy to return canonical_df."""
    ita_df = canonical_df[canonical_df["ref_area"] == "ITA"].copy()
    if len(ita_df) == 0:
        ita_df = canonical_df.copy()
        ita_df["ref_area"] = "ITA"
        ita_df["ref_area_name"] = "Italy"

    with (
        patch("src.processors.italy._oecd.fetch_country", return_value=(ita_df, "cached")),
        patch("src.processors.italy._eurostat.fetch_italy", return_value=(ita_df, "cached")),
        patch("src.processors.italy.load_italy_csv", return_value=ita_df),
    ):
        yield ita_df


# ── load_italy ────────────────────────────────────────────────────────────────

def test_load_italy_returns_dataframe(mock_italy_fetchers):
    df, sources = load_italy()
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


def test_load_italy_returns_canonical_columns(mock_italy_fetchers):
    df, sources = load_italy()
    for col in CANONICAL_COLUMNS:
        assert col in df.columns, f"Missing column: {col}"


def test_load_italy_returns_sources_list(mock_italy_fetchers):
    _, sources = load_italy()
    assert isinstance(sources, list)
    assert len(sources) > 0


def test_load_italy_oecd_fails_falls_back_to_csv(canonical_df):
    """If OECD raises FetchError, should still return data from fallback CSV."""
    ita_df = canonical_df[canonical_df["ref_area"] == "ITA"].copy()
    if len(ita_df) == 0:
        ita_df = canonical_df.copy()

    with (
        patch("src.processors.italy._oecd.fetch_country", side_effect=FetchError("OECD down")),
        patch("src.processors.italy._eurostat.fetch_italy", side_effect=FetchError("Eurostat down")),
        patch("src.processors.italy.load_italy_csv", return_value=ita_df),
    ):
        df, sources = load_italy()
        assert len(df) > 0
        assert "FALLBACK_CSV" in sources


def test_load_italy_all_fail_raises():
    """If all sources fail, RuntimeError should be raised."""
    with (
        patch("src.processors.italy._oecd.fetch_country", side_effect=FetchError("OECD down")),
        patch("src.processors.italy._eurostat.fetch_italy", side_effect=FetchError("Eurostat down")),
        patch("src.processors.italy.load_italy_csv", side_effect=Exception("CSV missing")),
    ):
        with pytest.raises(RuntimeError, match="All Italy data sources failed"):
            load_italy()


def test_load_italy_var_code_filter(mock_italy_fetchers):
    """Only requested var_codes should be present in merged result."""
    df, _ = load_italy(var_codes=["B11"], use_live=False)
    # B12 should not be in result when only B11 requested
    assert set(df["var_code"].unique()).issubset({"B11"})


def test_load_italy_no_b21_in_default(canonical_df):
    """B21 is not in default var_codes — no B21 rows should appear and no error raised."""
    ita_df = canonical_df.copy()
    ita_df["ref_area"] = "ITA"
    with (
        patch("src.processors.italy._oecd.fetch_country", return_value=(ita_df, "cached")),
        patch("src.processors.italy._eurostat.fetch_italy", return_value=(ita_df, "cached")),
        patch("src.processors.italy.load_italy_csv", return_value=ita_df),
    ):
        df, _ = load_italy()
        assert "B21" not in df["var_code"].values


def test_load_italy_use_live_false_skips_live_fetchers(canonical_df):
    """use_live=False should not call OECD or Eurostat."""
    ita_df = canonical_df.copy()
    ita_df["ref_area"] = "ITA"

    oecd_mock = MagicMock(return_value=(ita_df, "cached"))
    eurostat_mock = MagicMock(return_value=(ita_df, "cached"))

    with (
        patch("src.processors.italy._oecd.fetch_country", oecd_mock),
        patch("src.processors.italy._eurostat.fetch_italy", eurostat_mock),
        patch("src.processors.italy.load_italy_csv", return_value=ita_df),
    ):
        load_italy(use_live=False)
        oecd_mock.assert_not_called()
        eurostat_mock.assert_not_called()
