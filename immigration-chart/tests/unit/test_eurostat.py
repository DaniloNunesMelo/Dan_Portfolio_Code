"""Unit tests for src/fetchers/eurostat.py"""
import pytest
import responses as responses_lib

from src.fetchers.eurostat import EurostatFetcher, EUROSTAT_BASE, DATASET_TO_VAR
from src.fetchers.base import FetchError

CANONICAL_COLUMNS = [
    "ref_area", "ref_area_name", "counterpart", "counterpart_name",
    "time_period", "year", "quarter", "var_code", "metric",
    "sex", "gender", "area_name", "reg_name", "province",
    "imm_category", "obs_value", "obs_status", "source_dataset", "fetch_ts",
]


# ── _parse_json_stat ──────────────────────────────────────────────────────────

def test_parse_json_stat_canonical_schema(eurostat_json_stat_response):
    fetcher = EurostatFetcher()
    df = fetcher._parse_json_stat(eurostat_json_stat_response, "migr_imm1ctz", "IT", "T")
    for col in CANONICAL_COLUMNS:
        assert col in df.columns, f"Missing column: {col}"


def test_parse_json_stat_source_tag(eurostat_json_stat_response):
    fetcher = EurostatFetcher()
    df = fetcher._parse_json_stat(eurostat_json_stat_response, "migr_imm1ctz", "IT", "T")
    assert (df["source_dataset"] == "EUROSTAT").all()


def test_parse_json_stat_var_code(eurostat_json_stat_response):
    fetcher = EurostatFetcher()
    df = fetcher._parse_json_stat(eurostat_json_stat_response, "migr_imm1ctz", "IT", "T")
    assert (df["var_code"] == "B11").all()


def test_parse_json_stat_ref_area(eurostat_json_stat_response):
    fetcher = EurostatFetcher()
    df = fetcher._parse_json_stat(eurostat_json_stat_response, "migr_imm1ctz", "IT", "T")
    assert (df["ref_area"] == "ITA").all()


def test_parse_json_stat_obs_value_positive(eurostat_json_stat_response):
    fetcher = EurostatFetcher()
    df = fetcher._parse_json_stat(eurostat_json_stat_response, "migr_imm1ctz", "IT", "T")
    assert (df["obs_value"] > 0).all()


def test_parse_json_stat_empty_values_raises():
    data = {
        "id": ["sex", "citizen", "time"],
        "size": [1, 2, 2],
        "dimension": {
            "sex": {"category": {"index": {"T": 0}, "label": {"T": "Total"}}},
            "citizen": {"category": {"index": {"DEU": 0}, "label": {"DEU": "Germany"}}},
            "time": {"category": {"index": {"2010": 0}, "label": {"2010": "2010"}}},
        },
        "value": {},  # empty values
    }
    fetcher = EurostatFetcher()
    with pytest.raises(FetchError):
        fetcher._parse_json_stat(data, "migr_imm1ctz", "IT", "T")


def test_parse_json_stat_missing_structure_raises():
    fetcher = EurostatFetcher()
    with pytest.raises(FetchError):
        fetcher._parse_json_stat({}, "migr_imm1ctz", "IT", "T")


# ── dataset to var mapping ────────────────────────────────────────────────────

def test_dataset_to_var_imm():
    assert DATASET_TO_VAR["migr_imm1ctz"] == "B11"


def test_dataset_to_var_emi():
    assert DATASET_TO_VAR["migr_emi1ctz"] == "B12"


def test_dataset_to_var_acq():
    assert DATASET_TO_VAR["migr_acqctz"] == "B16"


# ── HTTP error handling ───────────────────────────────────────────────────────

@responses_lib.activate
def test_fetch_live_both_404_raises():
    url = EUROSTAT_BASE.format(dataset="migr_imm1ctz")
    responses_lib.add(responses_lib.GET, url, status=404)
    responses_lib.add(responses_lib.GET, url, status=404)
    fetcher = EurostatFetcher()
    with pytest.raises(FetchError):
        fetcher._fetch_live("migr_imm1ctz", "IT", "T")
