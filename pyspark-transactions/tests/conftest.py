"""Shared pytest fixtures.

The Spark session is created once per test run (``scope="session"``).
Helper functions let individual tests build small in-memory DataFrames
without reading files from disk.
"""

from __future__ import annotations

import shutil
import os

import pytest
from pyspark.sql import DataFrame, SparkSession

from e3_contracts_to_transactions.schemas import (
    CLAIM_SCHEMA,
    CONTRACT_SCHEMA,
    NSE_LOOKUP_SCHEMA,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    # Pre-flight: give a clear error instead of a JVM crash
    if not shutil.which("java") and not os.environ.get("JAVA_HOME"):
        pytest.skip(
            "Java not found. PySpark 4.0 requires JDK 17 or 21. "
            "Install and add to PATH or set JAVA_HOME."
        )

    session = (
        SparkSession.builder
        .appName("transactions_tests")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield session
    session.stop()


# ------------------------------------------------------------------
# Helper factories – every test builds only the rows it needs
# ------------------------------------------------------------------

def make_contracts(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    """Build a CONTRACT DataFrame from a list of tuples.

    Each tuple: (SOURCE_SYSTEM, CONTRACT_ID, CONTRACT_TYPE,
                  INSURED_PERIOD_FROM, INSURED_PERIOD_TO, CREATION_DATE)
    """
    return spark.createDataFrame(rows, schema=CONTRACT_SCHEMA)


def make_claims(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    """Build a CLAIM DataFrame from a list of tuples.

    Each tuple: (SOURCE_SYSTEM, CLAIM_ID, CONTRACT_SOURCE_SYSTEM,
                  CONTRACT_ID, CLAIM_TYPE, DATE_OF_LOSS, AMOUNT, CREATION_DATE)
    """
    return spark.createDataFrame(rows, schema=CLAIM_SCHEMA)


def make_nse_lookup(spark: SparkSession, pairs: list[tuple]) -> DataFrame:
    """Build an NSE lookup DataFrame from (CLAIM_ID, NSE_ID) pairs."""
    return spark.createDataFrame(pairs, schema=NSE_LOOKUP_SCHEMA)
