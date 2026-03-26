"""Unit tests for src/fetchers/oecd.py"""
import pytest
import responses as responses_lib

from src.fetchers.oecd import OECDFetcher, _safe_int, OECD_URL
from src.fetchers.base import FetchError

CANONICAL_COLUMNS = [
    "ref_area", "ref_area_name", "counterpart", "counterpart_name",
    "time_period", "year", "quarter", "var_code", "metric",
    "sex", "gender", "area_name", "reg_name", "province",
    "imm_category", "obs_value", "obs_status", "source_dataset", "fetch_ts",
]


# ── _parse_series_format ──────────────────────────────────────────────────────

def test_parse_series_format_canonical_schema(oecd_json_response):
    fetcher = OECDFetcher()
    df = fetcher._parse_series_format(oecd_json_response, "ITA", "B11", "T")
    for col in CANONICAL_COLUMNS:
        assert col in df.columns, f"Missing column: {col}"


def test_parse_series_format_filters_ref_area(oecd_json_response):
    fetcher = OECDFetcher()
    df = fetcher._parse_series_format(oecd_json_response, "ITA", "B11", "T")
    assert (df["ref_area"] == "ITA").all()


def test_parse_series_format_filters_var_code(oecd_json_response):
    fetcher = OECDFetcher()
    df = fetcher._parse_series_format(oecd_json_response, "ITA", "B11", "T")
    assert (df["var_code"] == "B11").all()


def test_parse_series_format_source_dataset(oecd_json_response):
    fetcher = OECDFetcher()
    df = fetcher._parse_series_format(oecd_json_response, "ITA", "B11", "T")
    assert (df["source_dataset"] == "OECD_MIG").all()


def test_parse_series_format_nonexistent_country_raises(oecd_json_response):
    fetcher = OECDFetcher()
    with pytest.raises(FetchError, match="not found"):
        fetcher._parse_series_format(oecd_json_response, "ZZZ", "B11", "T")


def test_parse_series_format_nonexistent_measure_raises(oecd_json_response):
    fetcher = OECDFetcher()
    with pytest.raises(FetchError, match="not found"):
        fetcher._parse_series_format(oecd_json_response, "ITA", "B99", "T")


def test_parse_series_format_obs_value_positive(oecd_json_response):
    fetcher = OECDFetcher()
    df = fetcher._parse_series_format(oecd_json_response, "ITA", "B11", "T")
    assert (df["obs_value"] > 0).all()


def test_parse_series_format_year_numeric(oecd_json_response):
    fetcher = OECDFetcher()
    df = fetcher._parse_series_format(oecd_json_response, "ITA", "B11", "T")
    import pandas as pd
    assert pd.api.types.is_numeric_dtype(df["year"])


# ── _safe_int ─────────────────────────────────────────────────────────────────

def test_safe_int_year():
    assert _safe_int("2022") == 2022


def test_safe_int_quarter():
    assert _safe_int("2022-Q3") == 2022


def test_safe_int_invalid():
    assert _safe_int("N/A") is None


def test_safe_int_none():
    assert _safe_int(None) is None


def test_safe_int_integer():
    assert _safe_int(2020) == 2020


# ── HTTP error handling ───────────────────────────────────────────────────────

@responses_lib.activate
def test_fetch_live_404_raises():
    responses_lib.add(responses_lib.GET, OECD_URL, status=404)
    fetcher = OECDFetcher()
    with pytest.raises(FetchError):
        fetcher._fetch_live("ITA", "B11", "T")


@responses_lib.activate
def test_fetch_live_422_raises():
    responses_lib.add(responses_lib.GET, OECD_URL, status=422)
    fetcher = OECDFetcher()
    with pytest.raises(FetchError):
        fetcher._fetch_live("ITA", "B11", "T")
