"""Unit tests for every transformation rule.

Design principles:
- Each test builds its own minimal in-memory DataFrame (no file I/O).
- Each test covers ONE mapping rule so failures pinpoint the broken rule.
- A final integration test exercises the full pipeline with all edge-cases.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, IntegerType, LongType, StringType, TimestampType

from e3_contracts_to_transactions.transform import (
    build_transactions,
    cast_conformed_value,
    extract_source_system_id,
    join_claims_to_contracts,
    join_nse_lookup,
    map_transaction_direction,
    map_transaction_type,
    parse_business_date,
    parse_creation_date,
    prepare_claims,
    prepare_contracts,
)

# Import the helpers directly (they live in conftest but are plain functions)
from tests.conftest import make_claims, make_contracts, make_nse_lookup


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _collect_column(df, col_name):
    """Return a plain Python list of values for *col_name*."""
    return [row[col_name] for row in df.select(col_name).collect()]


# ------------------------------------------------------------------
# Column mapping tests
# ------------------------------------------------------------------

class TestTransactionType:
    """TRANSACTION_TYPE: 2->Corporate, 1->Private, else->Unknown."""

    def test_corporate(self, spark):
        df = spark.createDataFrame([("2",)], ["CLAIM_TYPE"])
        result = df.select(map_transaction_type(F.col("CLAIM_TYPE")).alias("tt"))
        assert _collect_column(result, "tt") == ["Corporate"]

    def test_private(self, spark):
        df = spark.createDataFrame([("1",)], ["CLAIM_TYPE"])
        result = df.select(map_transaction_type(F.col("CLAIM_TYPE")).alias("tt"))
        assert _collect_column(result, "tt") == ["Private"]

    def test_unknown_for_empty(self, spark):
        df = spark.createDataFrame([("",)], ["CLAIM_TYPE"])
        result = df.select(map_transaction_type(F.col("CLAIM_TYPE")).alias("tt"))
        assert _collect_column(result, "tt") == ["Unknown"]

    def test_unknown_for_null(self, spark):
        from pyspark.sql.types import StructType, StructField
        schema = StructType([StructField("CLAIM_TYPE", StringType(), True)])
        df = spark.createDataFrame([(None,)], schema=schema)
        result = df.select(map_transaction_type(F.col("CLAIM_TYPE")).alias("tt"))
        assert _collect_column(result, "tt") == ["Unknown"]

    def test_unknown_for_unexpected_value(self, spark):
        df = spark.createDataFrame([("99",)], ["CLAIM_TYPE"])
        result = df.select(map_transaction_type(F.col("CLAIM_TYPE")).alias("tt"))
        assert _collect_column(result, "tt") == ["Unknown"]


class TestTransactionDirection:
    """TRANSACTION_DIRECTION: CL->COINSURANCE, RX->REINSURANCE, else->null."""

    def test_coinsurance(self, spark):
        df = spark.createDataFrame([("CL_123",)], ["CLAIM_ID"])
        result = df.select(map_transaction_direction(F.col("CLAIM_ID")).alias("td"))
        assert _collect_column(result, "td") == ["COINSURANCE"]

    def test_reinsurance(self, spark):
        df = spark.createDataFrame([("RX_456",)], ["CLAIM_ID"])
        result = df.select(map_transaction_direction(F.col("CLAIM_ID")).alias("td"))
        assert _collect_column(result, "td") == ["REINSURANCE"]

    def test_null_for_cx_prefix(self, spark):
        df = spark.createDataFrame([("CX_789",)], ["CLAIM_ID"])
        result = df.select(map_transaction_direction(F.col("CLAIM_ID")).alias("td"))
        assert _collect_column(result, "td") == [None]

    def test_null_for_u_prefix(self, spark):
        df = spark.createDataFrame([("U_000",)], ["CLAIM_ID"])
        result = df.select(map_transaction_direction(F.col("CLAIM_ID")).alias("td"))
        assert _collect_column(result, "td") == [None]


class TestSourceSystemId:
    """SOURCE_SYSTEM_ID: trailing digits of CLAIM_ID."""

    def test_cl_prefix(self, spark):
        df = spark.createDataFrame([("CL_68545123",)], ["CLAIM_ID"])
        result = df.select(extract_source_system_id(F.col("CLAIM_ID")).alias("ssid"))
        assert _collect_column(result, "ssid") == [68545123]

    def test_rx_prefix(self, spark):
        df = spark.createDataFrame([("RX_9845163",)], ["CLAIM_ID"])
        result = df.select(extract_source_system_id(F.col("CLAIM_ID")).alias("ssid"))
        assert _collect_column(result, "ssid") == [9845163]

    def test_u_prefix(self, spark):
        df = spark.createDataFrame([("U_7065313",)], ["CLAIM_ID"])
        result = df.select(extract_source_system_id(F.col("CLAIM_ID")).alias("ssid"))
        assert _collect_column(result, "ssid") == [7065313]

    def test_no_digits_returns_null(self, spark):
        """ANSI-safe: claim ID with no digits should return null, not throw."""
        df = spark.createDataFrame([("NO_DIGITS",)], ["CLAIM_ID"])
        result = df.select(extract_source_system_id(F.col("CLAIM_ID")).alias("ssid"))
        assert _collect_column(result, "ssid") == [None]


class TestDateParsing:
    """BUSINESS_DATE from dd.MM.yyyy and CREATION_DATE from dd.MM.yyyy HH:mm."""

    def test_business_date(self, spark):
        df = spark.createDataFrame([("14.02.2021",)], ["d"])
        result = df.select(parse_business_date(F.col("d")).alias("bd"))
        assert str(_collect_column(result, "bd")[0]) == "2021-02-14"

    def test_creation_date(self, spark):
        df = spark.createDataFrame([("17.01.2022 14:45",)], ["d"])
        result = df.select(
            parse_creation_date(F.col("d")).cast("string").alias("cd")
        )
        assert _collect_column(result, "cd") == ["2022-01-17 14:45:00"]

    def test_business_date_null_for_invalid(self, spark):
        df = spark.createDataFrame([("not-a-date",)], ["d"])
        result = df.select(parse_business_date(F.col("d")).alias("bd"))
        assert _collect_column(result, "bd") == [None]


class TestConformedValue:
    """CONFORMED_VALUE: AMOUNT as decimal(16,5)."""

    def test_decimal_amount(self, spark):
        df = spark.createDataFrame([("523.21",)], ["amt"])
        result = df.select(cast_conformed_value(F.col("amt")).alias("cv"))
        assert _collect_column(result, "cv") == [Decimal("523.21000")]

    def test_integer_amount(self, spark):
        df = spark.createDataFrame([("98465",)], ["amt"])
        result = df.select(cast_conformed_value(F.col("amt")).alias("cv"))
        assert _collect_column(result, "cv") == [Decimal("98465.00000")]

    def test_null_amount_returns_null(self, spark):
        """ANSI-safe: null AMOUNT should return null, not throw."""
        from pyspark.sql.types import StructType, StructField
        schema = StructType([StructField("amt", StringType(), True)])
        df = spark.createDataFrame([(None,)], schema=schema)
        result = df.select(cast_conformed_value(F.col("amt")).alias("cv"))
        assert _collect_column(result, "cv") == [None]

    def test_empty_amount_returns_null(self, spark):
        """ANSI-safe: empty string AMOUNT should return null, not throw."""
        df = spark.createDataFrame([("",)], ["amt"])
        result = df.select(cast_conformed_value(F.col("amt")).alias("cv"))
        assert _collect_column(result, "cv") == [None]


# ------------------------------------------------------------------
# Join tests
# ------------------------------------------------------------------

class TestContractJoin:
    """Left join: claims matched by source system + contract id."""

    def test_matching_contract(self, spark):
        contracts = make_contracts(spark, [
            ("Contract_SR_Europa_3", 100, "Direct", "", "", ""),
        ])
        claims = make_claims(spark, [
            ("Claim_SR", "CL_1", "Contract_SR_Europa_3", 100, "1", "01.01.2020", "10", "01.01.2020 10:00"),
        ])
        pc = prepare_contracts(contracts)
        pcl = prepare_claims(claims)
        joined = join_claims_to_contracts(pcl, pc)
        row = joined.collect()[0]
        assert row["CONTRACT_CONTRACT_ID"] == 100

    def test_no_match_returns_null(self, spark):
        contracts = make_contracts(spark, [
            ("Contract_SR_Europa_3", 100, "Direct", "", "", ""),
        ])
        claims = make_claims(spark, [
            ("Claim_SR", "CL_2", "Contract_SR_Europa_4", 100, "1", "01.01.2020", "10", "01.01.2020 10:00"),
        ])
        pc = prepare_contracts(contracts)
        pcl = prepare_claims(claims)
        joined = join_claims_to_contracts(pcl, pc)
        row = joined.collect()[0]
        assert row["CONTRACT_CONTRACT_ID"] is None

    def test_no_match_on_unknown_contract_id(self, spark):
        contracts = make_contracts(spark, [
            ("Contract_SR_Europa_3", 100, "Direct", "", "", ""),
        ])
        claims = make_claims(spark, [
            ("Claim_SR", "CL_3", "Contract_SR_Europa_3", 999, "2", "01.01.2020", "10", "01.01.2020 10:00"),
        ])
        pc = prepare_contracts(contracts)
        pcl = prepare_claims(claims)
        joined = join_claims_to_contracts(pcl, pc)
        row = joined.collect()[0]
        assert row["CONTRACT_CONTRACT_ID"] is None


class TestNseLookupJoin:
    """NSE lookup join attaches pre-computed NSE_ID."""

    def test_nse_id_attached(self, spark):
        claims = make_claims(spark, [
            ("Claim_SR", "CL_1", "X", 1, "1", "01.01.2020", "10", "01.01.2020 10:00"),
        ])
        lookup = make_nse_lookup(spark, [("CL_1", "abc123")])
        pcl = prepare_claims(claims)
        result = join_nse_lookup(pcl, lookup)
        assert result.collect()[0]["NSE_ID"] == "abc123"

    def test_missing_nse_returns_null(self, spark):
        claims = make_claims(spark, [
            ("Claim_SR", "CL_UNKNOWN", "X", 1, "1", "01.01.2020", "10", "01.01.2020 10:00"),
        ])
        lookup = make_nse_lookup(spark, [("CL_OTHER", "abc123")])
        pcl = prepare_claims(claims)
        result = join_nse_lookup(pcl, lookup)
        assert result.collect()[0]["NSE_ID"] is None


# ------------------------------------------------------------------
# Full integration test
# ------------------------------------------------------------------

class TestBuildTransactionsIntegration:
    """End-to-end: build_transactions with representative mock data."""

    @pytest.fixture()
    def result(self, spark):
        contracts = make_contracts(spark, [
            ("Contract_SR_Europa_3", 97563756, None, "01.01.2015", "01.01.2099", "17.01.2022 13:42"),
            ("Contract_SR_Europa_3", 13767503, "Reinsurance", "01.01.2015", "01.01.2099", "17.01.2022 13:42"),
            ("Contract_SR_Europa_3", 656948536, None, "01.01.2015", "01.01.2099", "17.01.2022 13:42"),
        ])
        claims = make_claims(spark, [
            # Matched corporate coinsurance
            ("Claim_SR", "CL_68545123", "Contract_SR_Europa_3", 97563756, "2", "14.02.2021", "523.21", "17.01.2022 14:45"),
            # Source system mismatch -> null contract id
            ("Claim_SR", "CL_962234", "Contract_SR_Europa_4", 408124123, "1", "30.01.2021", "52369.0", "17.01.2022 14:46"),
            # Empty claim type -> Unknown
            ("Claim_SR", "CL_895168", "Contract_SR_Europa_3", 13767503, "", "02.09.2020", "98465", "17.01.2022 14:45"),
            # CX prefix -> null direction
            ("Claim_SR", "CX_12066501", "Contract_SR_Europa_3", 656948536, "2", "04.01.2022", "9000", "17.01.2022 14:45"),
            # RX prefix -> REINSURANCE
            ("Claim_SR", "RX_9845163", "Contract_SR_Europa_3", 656948536, "2", "04.06.2015", "11000", "17.01.2022 14:45"),
            # U prefix -> null direction + contract id mismatch
            ("Claim_SR", "U_7065313", "Contract_SR_Europa_3", 46589516, "1", "29.09.2021", "11000", "17.01.2022 14:46"),
        ])
        nse = make_nse_lookup(spark, [
            ("CL_68545123", "hash_CL_68545123"),
            ("CL_962234", "hash_CL_962234"),
            ("CL_895168", "hash_CL_895168"),
            ("CX_12066501", "hash_CX_12066501"),
            ("RX_9845163", "hash_RX_9845163"),
            ("U_7065313", "hash_U_7065313"),
        ])
        return build_transactions(contracts, claims, nse_lookup_df=nse)

    def test_row_count(self, result):
        assert result.count() == 6

    def test_schema_types(self, result):
        fields = {f.name: f.dataType for f in result.schema.fields}
        assert isinstance(fields["CONTRACT_SOURCE_SYSTEM"], StringType)
        assert isinstance(fields["CONTRACT_SOURCE_SYSTEM_ID"], LongType)
        assert isinstance(fields["SOURCE_SYSTEM_ID"], IntegerType)
        assert isinstance(fields["TRANSACTION_TYPE"], StringType)
        assert isinstance(fields["TRANSACTION_DIRECTION"], StringType)
        assert isinstance(fields["CONFORMED_VALUE"], DecimalType)
        assert isinstance(fields["BUSINESS_DATE"], DateType)
        assert isinstance(fields["CREATION_DATE"], TimestampType)
        assert isinstance(fields["SYSTEM_TIMESTAMP"], TimestampType)
        assert isinstance(fields["NSE_ID"], StringType)

    def test_all_rows_have_source_system_europe_3(self, result):
        values = _collect_column(result, "CONTRACT_SOURCE_SYSTEM")
        assert all(v == "Europe 3" for v in values)

    def test_matched_corporate_coinsurance(self, result):
        # Cast timestamps to string inside Spark (respects session TZ = UTC)
        # to avoid Python's local-timezone str() conversion.
        rows_df = result.withColumn(
            "CREATION_DATE_STR",
            F.col("CREATION_DATE").cast("string"),
        )
        rows = {r["SOURCE_SYSTEM_ID"]: r for r in rows_df.collect()}
        r = rows[68545123]
        assert r["CONTRACT_SOURCE_SYSTEM_ID"] == 97563756
        assert r["TRANSACTION_TYPE"] == "Corporate"
        assert r["TRANSACTION_DIRECTION"] == "COINSURANCE"
        assert r["CONFORMED_VALUE"] == Decimal("523.21000")
        assert str(r["BUSINESS_DATE"]) == "2021-02-14"
        assert r["CREATION_DATE_STR"] == "2022-01-17 14:45:00"
        assert r["NSE_ID"] == "hash_CL_68545123"

    def test_source_system_mismatch_null_contract(self, result):
        rows = {r["SOURCE_SYSTEM_ID"]: r for r in result.collect()}
        r = rows[962234]
        assert r["CONTRACT_SOURCE_SYSTEM_ID"] is None
        assert r["TRANSACTION_TYPE"] == "Private"
        assert r["TRANSACTION_DIRECTION"] == "COINSURANCE"

    def test_empty_claim_type_is_unknown(self, result):
        rows = {r["SOURCE_SYSTEM_ID"]: r for r in result.collect()}
        r = rows[895168]
        assert r["TRANSACTION_TYPE"] == "Unknown"

    def test_cx_prefix_direction_is_null(self, result):
        rows = {r["SOURCE_SYSTEM_ID"]: r for r in result.collect()}
        r = rows[12066501]
        assert r["TRANSACTION_DIRECTION"] is None

    def test_rx_prefix_direction_is_reinsurance(self, result):
        rows = {r["SOURCE_SYSTEM_ID"]: r for r in result.collect()}
        r = rows[9845163]
        assert r["TRANSACTION_DIRECTION"] == "REINSURANCE"

    def test_unknown_prefix_null_direction_and_null_contract(self, result):
        rows = {r["SOURCE_SYSTEM_ID"]: r for r in result.collect()}
        r = rows[7065313]
        assert r["TRANSACTION_DIRECTION"] is None
        assert r["CONTRACT_SOURCE_SYSTEM_ID"] is None
