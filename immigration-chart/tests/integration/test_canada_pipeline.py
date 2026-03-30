"""Integration tests for src/processors/canada.py"""
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from src.fetchers.base import FetchError
from src.processors.canada import load_canada

CANONICAL_COLUMNS = [
    "ref_area", "ref_area_name", "counterpart", "counterpart_name",
    "time_period", "year", "quarter", "var_code", "metric",
    "sex", "gender", "area_name", "reg_name", "province",
    "imm_category", "obs_value", "obs_status", "source_dataset", "fetch_ts",
]


@pytest.fixture
def can_df(canonical_df):
    df = canonical_df[canonical_df["ref_area"] == "CAN"].copy()
    if len(df) == 0:
        df = canonical_df.copy()
        df["ref_area"] = "CAN"
        df["ref_area_name"] = "Canada"
    return df


@pytest.fixture
def mock_canada_fetchers(can_df):
    with (
        patch("src.processors.canada._oecd.fetch_country", return_value=(can_df, "cached")),
        patch("src.processors.canada._ircc.fetch_by_citizenship", return_value=(can_df, "cached")),
        patch("src.processors.canada._ircc.fetch_by_province_category", return_value=(can_df, "cached")),
        patch("src.processors.canada.load_canada_xlsx", return_value=can_df),
    ):
        yield can_df


# ── load_canada ────────────────────────────────────────────────────────────────

def test_load_canada_returns_dataframe(mock_canada_fetchers):
    df, sources = load_canada()
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


def test_load_canada_returns_canonical_columns(mock_canada_fetchers):
    df, _ = load_canada()
    for col in CANONICAL_COLUMNS:
        assert col in df.columns, f"Missing column: {col}"


def test_load_canada_returns_sources_list(mock_canada_fetchers):
    _, sources = load_canada()
    assert isinstance(sources, list)
    assert len(sources) > 0


def test_load_canada_pr_only_skips_oecd(can_df):
    """When only PR is requested, OECD should not be called."""
    oecd_mock = MagicMock(return_value=(can_df, "cached"))
    ircc_mock = MagicMock(return_value=(can_df, "cached"))

    with (
        patch("src.processors.canada._oecd.fetch_country", oecd_mock),
        patch("src.processors.canada._ircc.fetch_by_citizenship", ircc_mock),
        patch("src.processors.canada._ircc.fetch_by_province_category", MagicMock(return_value=(can_df, "cached"))),
        patch("src.processors.canada.load_canada_xlsx", return_value=can_df),
    ):
        load_canada(var_codes=["PR"])
        oecd_mock.assert_not_called()


def test_load_canada_historical_fallback_called(can_df):
    """include_historical=True should call load_canada_xlsx."""
    xlsx_mock = MagicMock(return_value=can_df)

    with (
        patch("src.processors.canada._oecd.fetch_country", return_value=(can_df, "cached")),
        patch("src.processors.canada._ircc.fetch_by_citizenship", return_value=(can_df, "cached")),
        patch("src.processors.canada._ircc.fetch_by_province_category", return_value=(can_df, "cached")),
        patch("src.processors.canada.load_canada_xlsx", xlsx_mock),
    ):
        load_canada(include_historical=True)
        xlsx_mock.assert_called_once()


def test_load_canada_all_fail_raises():
    with (
        patch("src.processors.canada._oecd.fetch_country", side_effect=FetchError("OECD down")),
        patch("src.processors.canada._ircc.fetch_by_citizenship", side_effect=FetchError("IRCC down")),
        patch("src.processors.canada._ircc.fetch_by_province_category", side_effect=FetchError("IRCC down")),
        patch("src.processors.canada.load_canada_xlsx", side_effect=Exception("XLSX missing")),
    ):
        with pytest.raises(RuntimeError, match="All Canada data sources failed"):
            load_canada()


def test_load_canada_no_b21_in_default(can_df):
    """B21 is not in default var_codes — no B21 rows should appear and no error raised."""
    with (
        patch("src.processors.canada._oecd.fetch_country", return_value=(can_df, "cached")),
        patch("src.processors.canada._ircc.fetch_by_citizenship", return_value=(can_df, "cached")),
        patch("src.processors.canada._ircc.fetch_by_province_category", return_value=(can_df, "cached")),
        patch("src.processors.canada.load_canada_xlsx", return_value=can_df),
    ):
        df, _ = load_canada()
        assert "B21" not in df["var_code"].values


def test_load_canada_use_live_false(can_df):
    oecd_mock = MagicMock(return_value=(can_df, "cached"))
    ircc_mock = MagicMock(return_value=(can_df, "cached"))

    with (
        patch("src.processors.canada._oecd.fetch_country", oecd_mock),
        patch("src.processors.canada._ircc.fetch_by_citizenship", ircc_mock),
        patch("src.processors.canada._ircc.fetch_by_province_category", ircc_mock),
        patch("src.processors.canada.load_canada_xlsx", return_value=can_df),
    ):
        load_canada(use_live=False, include_historical=True)
        oecd_mock.assert_not_called()
        ircc_mock.assert_not_called()
