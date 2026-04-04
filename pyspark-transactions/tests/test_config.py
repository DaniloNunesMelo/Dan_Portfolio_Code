"""Tests for the config / parameter loader."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest

from contracts_to_transactions.config import load_parameters


@pytest.fixture()
def tmp_config(tmp_path):
    """Helper that writes a YAML string to a temp file and returns the path."""

    def _write(content: str) -> Path:
        p = tmp_path / "params.yaml"
        p.write_text(dedent(content), encoding="utf-8")
        return p

    return _write


class TestLoadParameters:
    def test_loads_valid_config(self, tmp_config):
        p = tmp_config("""\
            source_system: "Europe 3"
            transaction_type_mapping:
              "1": "Private"
              "2": "Corporate"
            transaction_type_default: "Unknown"
            transaction_direction_mapping:
              "CL": "COINSURANCE"
            date_of_loss_format: "dd.MM.yyyy"
            creation_date_format: "dd.MM.yyyy HH:mm"
        """)
        cfg = load_parameters(p)
        assert cfg["source_system"] == "Europe 3"
        assert cfg["transaction_type_mapping"]["2"] == "Corporate"

    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Config file not found"):
            load_parameters(tmp_path / "nonexistent.yaml")

    def test_missing_required_key_raises(self, tmp_config):
        p = tmp_config("""\
            source_system: "Europe 3"
        """)
        with pytest.raises(ValueError, match="missing required keys"):
            load_parameters(p)

    def test_env_variable_override(self, tmp_config, monkeypatch):
        p = tmp_config("""\
            source_system: "From Env"
            transaction_type_mapping: {}
            transaction_type_default: "X"
            transaction_direction_mapping: {}
            date_of_loss_format: "dd.MM.yyyy"
            creation_date_format: "dd.MM.yyyy HH:mm"
        """)
        monkeypatch.setenv("PIPELINE_CONFIG", str(p))
        cfg = load_parameters()  # no explicit path
        assert cfg["source_system"] == "From Env"

    def test_extra_keys_are_preserved(self, tmp_config):
        p = tmp_config("""\
            source_system: "Europe 3"
            transaction_type_mapping: {}
            transaction_type_default: "X"
            transaction_direction_mapping: {}
            date_of_loss_format: "dd.MM.yyyy"
            creation_date_format: "dd.MM.yyyy HH:mm"
            custom_field: 42
        """)
        cfg = load_parameters(p)
        assert cfg["custom_field"] == 42

    def test_transaction_type_mapping_with_three_entries(self, tmp_config):
        """Verifies a new claim type can be added without code changes."""
        p = tmp_config("""\
            source_system: "Europe 3"
            transaction_type_mapping:
              "1": "Private"
              "2": "Corporate"
              "3": "Government"
            transaction_type_default: "Unknown"
            transaction_direction_mapping: {}
            date_of_loss_format: "dd.MM.yyyy"
            creation_date_format: "dd.MM.yyyy HH:mm"
        """)
        cfg = load_parameters(p)
        assert len(cfg["transaction_type_mapping"]) == 3
        assert cfg["transaction_type_mapping"]["3"] == "Government"
