"""Unit tests for src/processors/merge.py"""
from datetime import datetime, timezone

import pandas as pd
import pytest

from src.processors.merge import merge_sources, get_coverage_info, CANONICAL_COLUMNS, DEDUP_KEY


def _make_row(
    ref_area="ITA", counterpart="DEU", time_period="2010",
    var_code="B11", sex="T", source_dataset="OECD_MIG", obs_value=1000.0,
) -> dict:
    return {
        "ref_area": ref_area,
        "ref_area_name": "Italy",
        "counterpart": counterpart,
        "counterpart_name": "Germany",
        "time_period": time_period,
        "year": int(time_period),
        "quarter": None,
        "var_code": var_code,
        "metric": "Inflows of Foreign Population",
        "sex": sex,
        "gender": "Total",
        "area_name": None,
        "reg_name": None,
        "province": None,
        "imm_category": None,
        "obs_value": obs_value,
        "obs_status": None,
        "source_dataset": source_dataset,
        "fetch_ts": datetime(2024, 1, 1, tzinfo=timezone.utc),
    }


def _df(*rows) -> pd.DataFrame:
    return pd.DataFrame([_make_row(**r) for r in rows])


# ── merge_sources ─────────────────────────────────────────────────────────────

def test_merge_empty_list():
    result = merge_sources([])
    assert list(result.columns) == CANONICAL_COLUMNS
    assert len(result) == 0


def test_merge_single_source():
    df = _df({"source_dataset": "OECD_MIG", "obs_value": 500.0})
    result = merge_sources([df])
    assert len(result) == 1
    assert result.iloc[0]["obs_value"] == 500.0


def test_merge_priority_oecd_over_fallback():
    """OECD_MIG should win over FALLBACK_CSV for the same key."""
    oecd_row = _df({"source_dataset": "OECD_MIG", "obs_value": 999.0})
    fallback_row = _df({"source_dataset": "FALLBACK_CSV", "obs_value": 111.0})
    result = merge_sources([fallback_row, oecd_row])
    assert len(result) == 1
    assert result.iloc[0]["obs_value"] == 999.0
    assert result.iloc[0]["source_dataset"] == "OECD_MIG"


def test_merge_priority_eurostat_over_ircc():
    eurostat_row = _df({"source_dataset": "EUROSTAT", "obs_value": 800.0})
    ircc_row = _df({"source_dataset": "IRCC", "obs_value": 200.0})
    result = merge_sources([ircc_row, eurostat_row])
    assert len(result) == 1
    assert result.iloc[0]["source_dataset"] == "EUROSTAT"


def test_merge_priority_ircc_over_fallback_csv():
    ircc_row = _df({"source_dataset": "IRCC", "obs_value": 400.0})
    fallback_row = _df({"source_dataset": "FALLBACK_CSV", "obs_value": 100.0})
    result = merge_sources([fallback_row, ircc_row])
    assert len(result) == 1
    assert result.iloc[0]["source_dataset"] == "IRCC"


def test_merge_no_dedup_across_var_codes():
    """Rows for different var_codes must NOT be deduped against each other."""
    b11 = _df({"var_code": "B11", "source_dataset": "OECD_MIG"})
    b12 = _df({"var_code": "B12", "source_dataset": "OECD_MIG"})
    result = merge_sources([b11, b12])
    assert len(result) == 2


def test_merge_missing_column_filled(canonical_df):
    """Source DF missing area_name gets None column added."""
    df = canonical_df.drop(columns=["area_name"])
    result = merge_sources([df])
    assert "area_name" in result.columns


def test_merge_year_numeric_after_merge():
    df = _df({})
    result = merge_sources([df])
    assert pd.api.types.is_integer_dtype(result["year"].dtype)


def test_merge_obs_value_numeric():
    df = _df({"obs_value": 42.0})
    result = merge_sources([df])
    assert pd.api.types.is_float_dtype(result["obs_value"])


def test_merge_none_df_ignored():
    df = _df({})
    result = merge_sources([None, df, None])
    assert len(result) == 1


def test_merge_empty_df_ignored():
    df = _df({})
    empty = pd.DataFrame(columns=CANONICAL_COLUMNS)
    result = merge_sources([empty, df])
    assert len(result) == 1


# ── get_coverage_info ─────────────────────────────────────────────────────────

def test_get_coverage_info_columns(canonical_df):
    result = get_coverage_info(canonical_df)
    for col in ["source_dataset", "ref_area", "min_year", "max_year", "row_count"]:
        assert col in result.columns


def test_get_coverage_info_empty():
    result = get_coverage_info(pd.DataFrame(columns=CANONICAL_COLUMNS))
    assert len(result) == 0


def test_get_coverage_info_row_count(canonical_df):
    result = get_coverage_info(canonical_df)
    # canonical_df has 2 ITA rows and 1 CAN row
    total_rows = result["row_count"].sum()
    assert total_rows == len(canonical_df)
