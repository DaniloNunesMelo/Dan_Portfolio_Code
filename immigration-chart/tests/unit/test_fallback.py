"""Unit tests for src/fetchers/fallback.py"""
from pathlib import Path

import pandas as pd
import pytest

from src.fetchers.fallback import (
    discover_files,
    load_fallback,
    load_italy_csv,
    load_canada_xlsx,
)

CANONICAL_COLUMNS = [
    "ref_area", "ref_area_name", "counterpart", "counterpart_name",
    "time_period", "year", "quarter", "var_code", "metric",
    "sex", "gender", "area_name", "reg_name", "province",
    "imm_category", "obs_value", "obs_status", "source_dataset", "fetch_ts",
]


# ── discover_files ────────────────────────────────────────────────────────────

def test_discover_files_exact_match(italy_csv_tmp, monkeypatch, tmp_path):
    import src.fetchers.fallback as fb
    # DATA_RAW is already monkeypatched by the fixture
    results = discover_files("oecd", "italy")
    assert len(results) == 1
    assert results[0].name == "oecd_italy.csv"


def test_discover_files_no_match(tmp_path, monkeypatch):
    import src.fetchers.fallback as fb
    monkeypatch.setattr(fb, "DATA_RAW", tmp_path)
    results = discover_files("oecd", "nonexistent")
    assert results == []


def test_discover_files_country_wildcard(italy_csv_tmp, monkeypatch, tmp_path):
    import src.fetchers.fallback as fb
    # Any source for 'italy'
    results = discover_files(country="italy")
    assert any("italy" in p.name for p in results)


def test_discover_files_src_wildcard(italy_csv_tmp, monkeypatch, tmp_path):
    import src.fetchers.fallback as fb
    results = discover_files(src="oecd")
    assert any("oecd" in p.name for p in results)


# ── load_italy_csv ────────────────────────────────────────────────────────────

def test_load_italy_csv_schema(italy_csv_tmp):
    df = load_italy_csv()
    for col in CANONICAL_COLUMNS:
        assert col in df.columns, f"Missing column: {col}"


def test_load_italy_csv_source_tag(italy_csv_tmp):
    df = load_italy_csv()
    assert (df["source_dataset"] == "FALLBACK_CSV").all()


def test_load_italy_csv_numeric_value(italy_csv_tmp):
    df = load_italy_csv()
    assert pd.api.types.is_float_dtype(df["obs_value"])
    assert df["obs_value"].notna().all()


def test_load_italy_csv_var_code_mapped(italy_csv_tmp):
    df = load_italy_csv()
    valid_codes = {"B11", "B12", "B13", "B14", "B15", "B16", "B21"}
    assert set(df["var_code"].unique()).issubset(valid_codes)


def test_load_italy_csv_gender_mapped(italy_csv_tmp):
    df = load_italy_csv()
    valid_genders = {"Total", "Male", "Female"}
    assert set(df["gender"].unique()).issubset(valid_genders)


def test_load_italy_csv_ref_area(italy_csv_tmp):
    df = load_italy_csv()
    assert (df["ref_area"] == "ITA").all()


# ── load_canada_csv ──────────────────────────────────────────────────────────

def test_load_canada_csv_schema(canada_csv_tmp):
    df = load_canada_xlsx()
    for col in CANONICAL_COLUMNS:
        assert col in df.columns, f"Missing column: {col}"


def test_load_canada_csv_source_tag(canada_csv_tmp):
    df = load_canada_xlsx()
    assert (df["source_dataset"] == "FALLBACK_CSV").all()


def test_load_canada_csv_year_range(canada_csv_tmp):
    df = load_canada_xlsx()
    years = df["year"].dropna().astype(int)
    assert (years >= 1900).all()
    assert (years <= 2100).all()


def test_load_canada_csv_positive_values(canada_csv_tmp):
    df = load_canada_xlsx()
    assert (df["obs_value"] > 0).all()


def test_load_canada_csv_ref_area(canada_csv_tmp):
    df = load_canada_xlsx()
    assert (df["ref_area"] == "CAN").all()


# ── load_fallback errors ──────────────────────────────────────────────────────

def test_load_fallback_missing_file_raises(tmp_path, monkeypatch):
    import src.fetchers.fallback as fb
    monkeypatch.setattr(fb, "DATA_RAW", tmp_path)
    with pytest.raises(FileNotFoundError):
        load_fallback("oecd", "nonexistent")


def test_load_fallback_unknown_extension_raises(tmp_path, monkeypatch):
    import src.fetchers.fallback as fb
    monkeypatch.setattr(fb, "DATA_RAW", tmp_path)
    # Create a .txt file that matches the pattern
    (tmp_path / "foo_bar.txt").write_text("data")
    with pytest.raises(ValueError, match="Unsupported file extension"):
        load_fallback("foo", "bar")
