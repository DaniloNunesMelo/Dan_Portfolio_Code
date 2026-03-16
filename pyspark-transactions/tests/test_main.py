"""Tests for main.py — CLI entrypoint & orchestration.

Targets previously uncovered lines 30-58, 67-77, 113-132, 136-139
from the coverage report.
"""

from __future__ import annotations

from textwrap import dedent
from unittest.mock import patch, MagicMock

import pytest
from pyspark.sql import SparkSession

from e3_contracts_to_transactions.main import (
    create_spark_session,
    main,
    parse_args,
    run_pipeline,
)


# ── parse_args ───────────────────────────────────────────────────


class TestParseArgs:
    def test_required_args(self):
        args = parse_args(["--contracts", "c.csv", "--claims", "cl.csv"])
        assert args.contracts == "c.csv"
        assert args.claims == "cl.csv"
        assert args.output == "output/TRANSACTIONS.csv"  # default
        assert args.config is None  # default
        assert args.log_level == "INFO"  # default

    def test_all_args(self):
        args = parse_args([
            "--contracts", "c.csv",
            "--claims", "cl.csv",
            "--output", "out.csv",
            "--config", "my.yaml",
            "--log-level", "DEBUG",
        ])
        assert args.output == "out.csv"
        assert args.config == "my.yaml"
        assert args.log_level == "DEBUG"

    def test_missing_required_contracts(self):
        with pytest.raises(SystemExit):
            parse_args(["--claims", "cl.csv"])

    def test_missing_required_claims(self):
        with pytest.raises(SystemExit):
            parse_args(["--contracts", "c.csv"])

    def test_invalid_log_level(self):
        with pytest.raises(SystemExit):
            parse_args(["--contracts", "c.csv", "--claims", "cl.csv", "--log-level", "INVALID"])


# ── create_spark_session ─────────────────────────────────────────


class TestCreateSparkSession:
    def test_returns_spark_session(self, spark):
        """Use the existing session from conftest rather than creating a new one."""
        # Just verify the fixture-provided session is valid
        assert isinstance(spark, SparkSession)
        assert spark.sparkContext is not None

    def test_custom_app_name(self):
        # Don't actually create a new session in tests, but verify the function
        # is callable with a custom name. We mock the builder to avoid side effects.
        with patch("e3_contracts_to_transactions.main.SparkSession") as mock_ss:
            mock_builder = MagicMock()
            mock_ss.builder = mock_builder
            mock_builder.appName.return_value = mock_builder
            mock_builder.master.return_value = mock_builder
            mock_builder.getOrCreate.return_value = MagicMock(spec=SparkSession)

            create_spark_session("CustomApp")

            mock_builder.appName.assert_called_once_with("CustomApp")
            mock_builder.master.assert_called_once_with("local[*]")


# ── run_pipeline ─────────────────────────────────────────────────


class TestRunPipeline:
    @pytest.fixture()
    def pipeline_files(self, tmp_path):
        """Create minimal contracts + claims CSVs and a config dict."""
        contracts = tmp_path / "contracts.csv"
        contracts.write_text(
            "SOURCE_SYSTEM,CONTRACT_ID\n"
            "SYS_A,100\n",
            encoding="utf-8",
        )
        claims = tmp_path / "claims.csv"
        claims.write_text(
            "SOURCE_SYSTEM,CLAIM_ID,CONTRACT_SOURCE_SYSTEM,CONTRACT_ID,"
            "CLAIM_TYPE,DATE_OF_LOSS,AMOUNT,CREATION_DATE\n"
            "Claim,CL_123,SYS_A,100,2,14.02.2021,500,17.01.2022 14:45\n",
            encoding="utf-8",
        )
        output = tmp_path / "out" / "TRANSACTIONS.csv"

        config = {
            "source_system": "Europe 3",
            "transaction_type_mapping": {"1": "Private", "2": "Corporate"},
            "transaction_type_default": "Unknown",
            "transaction_direction_mapping": {"CL": "COINSURANCE", "RX": "REINSURANCE"},
            "date_of_loss_format": "dd.MM.yyyy",
            "creation_date_format": "dd.MM.yyyy HH:mm",
            "hashify_base_url": "https://api.hashify.net/hash/md4/hex",
            "hashify_response_field": "Digest",
            "claim_contract_join": {
                "claim_contract_id_col": "CONTRACT_ID",
                "claim_source_system_col": "CONTRACT_SOURCE_SYSTEM",
                "contract_id_col": "CONTRACT_ID",
                "contract_source_system_col": "SOURCE_SYSTEM",
            },
            "output_header": True,
            "output_delimiter": ",",
        }
        return contracts, claims, output, config

    def test_end_to_end_produces_file(self, spark, pipeline_files):
        contracts, claims, output, config = pipeline_files

        # Mock the API call so we don't hit the network
        with patch(
            "e3_contracts_to_transactions.main.make_hashify_fn",
            return_value=lambda cid: f"hash_{cid}",
        ):
            run_pipeline(spark, config, str(contracts), str(claims), str(output))

        assert output.exists()
        content = output.read_text(encoding="utf-8")
        assert "Europe 3" in content
        assert "Corporate" in content
        assert "COINSURANCE" in content

    def test_output_directory_created(self, spark, pipeline_files):
        contracts, claims, output, config = pipeline_files
        # Output dir doesn't exist yet
        assert not output.parent.exists()

        with patch(
            "e3_contracts_to_transactions.main.make_hashify_fn",
            return_value=lambda cid: f"h_{cid}",
        ):
            run_pipeline(spark, config, str(contracts), str(claims), str(output))

        assert output.parent.exists()

    def test_missing_contracts_file_raises(self, spark, pipeline_files):
        _, claims, output, config = pipeline_files

        with patch(
            "e3_contracts_to_transactions.main.make_hashify_fn",
            return_value=lambda cid: "x",
        ):
            with pytest.raises(FileNotFoundError):
                run_pipeline(spark, config, "/no/such/file.csv", str(claims), str(output))

    def test_missing_claims_file_raises(self, spark, pipeline_files):
        contracts, _, output, config = pipeline_files

        with patch(
            "e3_contracts_to_transactions.main.make_hashify_fn",
            return_value=lambda cid: "x",
        ):
            with pytest.raises(FileNotFoundError):
                run_pipeline(spark, config, str(contracts), "/no/such/file.csv", str(output))


# ── main() entry point ───────────────────────────────────────────


class TestMain:
    def test_main_with_valid_args(self, tmp_path):
        """Full integration through main() with all dependencies mocked."""
        contracts = tmp_path / "contracts.csv"
        contracts.write_text("SOURCE_SYSTEM,CONTRACT_ID\nS,1\n", encoding="utf-8")
        claims = tmp_path / "claims.csv"
        claims.write_text(
            "SOURCE_SYSTEM,CLAIM_ID,CONTRACT_SOURCE_SYSTEM,CONTRACT_ID,"
            "CLAIM_TYPE,DATE_OF_LOSS,AMOUNT,CREATION_DATE\n"
            "S,CL_1,S,1,1,01.01.2020,10,01.01.2020 10:00\n",
            encoding="utf-8",
        )
        config_path = tmp_path / "params.yaml"
        config_path.write_text(dedent("""\
            source_system: "Europe 3"
            transaction_type_mapping:
              "1": "Private"
              "2": "Corporate"
            transaction_type_default: "Unknown"
            transaction_direction_mapping:
              "CL": "COINSURANCE"
              "RX": "REINSURANCE"
            date_of_loss_format: "dd.MM.yyyy"
            creation_date_format: "dd.MM.yyyy HH:mm"
            hashify_base_url: "https://api.hashify.net/hash/md4/hex"
            hashify_response_field: "Digest"
            claim_contract_join:
              claim_contract_id_col: "CONTRACT_ID"
              claim_source_system_col: "CONTRACT_SOURCE_SYSTEM"
              contract_id_col: "CONTRACT_ID"
              contract_source_system_col: "SOURCE_SYSTEM"
            output_header: true
            output_delimiter: ","
        """), encoding="utf-8")
        output = tmp_path / "TRANSACTIONS.csv"

        with patch(
            "e3_contracts_to_transactions.main.make_hashify_fn",
            return_value=lambda cid: "mocked_hash",
        ):
            main([
                "--contracts", str(contracts),
                "--claims", str(claims),
                "--output", str(output),
                "--config", str(config_path),
                "--log-level", "WARNING",
            ])

        assert output.exists()
        content = output.read_text(encoding="utf-8")
        assert "mocked_hash" in content

    def test_main_bad_config_path(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            main([
                "--contracts", "c.csv",
                "--claims", "cl.csv",
                "--config", str(tmp_path / "no_such.yaml"),
            ])
