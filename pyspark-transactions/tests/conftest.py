"""Shared test fixtures."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped local Spark for all tests."""
    session = (
        SparkSession.builder
        .appName("Europe3_Tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def default_config() -> dict:
    """A minimal config dict matching production defaults."""
    return {
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
