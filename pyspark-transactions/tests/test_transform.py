"""Unit tests for every transformation rule in transform.py."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from e3_contracts_to_transactions.transform import (
    add_business_date,
    add_conformed_value,
    add_contract_source_system_id,
    add_creation_date,
    add_nse_id,
    add_source_system,
    add_source_system_id,
    add_system_timestamp,
    add_transaction_direction,
    add_transaction_type,
    build_transactions,
)

# -- Helpers ---------------------------------------------------

_CLAIM_SCHEMA = StructType(
    [
        StructField("CLAIM_ID", StringType()),
        StructField("CONTRACT_SOURCE_SYSTEM", StringType()),
        StructField("CONTRACT_ID", StringType()),
        StructField("CLAIM_TYPE", StringType()),
        StructField("DATE_OF_LOSS", StringType()),
        StructField("AMOUNT", StringType()),
        StructField("CREATION_DATE", StringType()),
    ]
)

_CONTRACT_SCHEMA = StructType(
    [
        StructField("SOURCE_SYSTEM", StringType()),
        StructField("CONTRACT_ID", StringType()),
    ]
)

# Default row template: (claim_id, css, cid, ct, dol, amt, cd)
_ROW = ("CL_1", "S", "1", "1", "01.01.2020", "10", "01.01.2020 10:00")


def _claims_df(
    spark: SparkSession,
    rows: list[tuple],
) -> DataFrame:
    return spark.createDataFrame(rows, _CLAIM_SCHEMA)


def _contracts_df(
    spark: SparkSession,
    rows: list[tuple],
) -> DataFrame:
    return spark.createDataFrame(rows, _CONTRACT_SCHEMA)


# -- add_source_system -----------------------------------------


class TestAddSourceSystem:
    def test_literal_value(self, spark):
        df = _claims_df(spark, [_ROW])
        result = add_source_system(df, "Europe 3")
        row = result.collect()[0]
        assert row["CONTRACT_SOURCE_SYSTEM"] == "Europe 3"

    def test_custom_source_system(self, spark):
        df = _claims_df(spark, [_ROW])
        result = add_source_system(df, "Asia 1")
        row = result.collect()[0]
        assert row["CONTRACT_SOURCE_SYSTEM"] == "Asia 1"


# -- add_source_system_id --------------------------------------


class TestAddSourceSystemId:
    @pytest.mark.parametrize(
        "claim_id, expected",
        [
            ("CL_68545123", 68545123),
            ("RX_9845163", 9845163),
            ("CX_12066501", 12066501),
            ("U_7065313", 7065313),
            ("A_123", 123),
        ],
    )
    def test_extracts_numeric_suffix(
        self, spark, claim_id, expected
    ):
        row = (
            claim_id,
            "S",
            "1",
            "1",
            "01.01.2020",
            "10",
            "01.01.2020 10:00",
        )
        df = _claims_df(spark, [row])
        result = add_source_system_id(df)
        assert result.collect()[0]["SOURCE_SYSTEM_ID"] == expected

    def test_no_underscore_returns_null(self, spark):
        row = (
            "NOUNDERSCORE",
            "S",
            "1",
            "1",
            "01.01.2020",
            "10",
            "01.01.2020 10:00",
        )
        df = _claims_df(spark, [row])
        result = add_source_system_id(df)
        assert result.collect()[0]["SOURCE_SYSTEM_ID"] is None


# -- add_transaction_type --------------------------------------


class TestAddTransactionType:
    def _row(self, claim_type):
        return (
            "CL_1",
            "S",
            "1",
            claim_type,
            "01.01.2020",
            "10",
            "01.01.2020 10:00",
        )

    def test_corporate(self, spark, default_config):
        df = _claims_df(spark, [self._row("2")])
        result = add_transaction_type(df, default_config)
        assert (
            result.collect()[0]["TRANSACTION_TYPE"]
            == "Corporate"
        )

    def test_private(self, spark, default_config):
        df = _claims_df(spark, [self._row("1")])
        result = add_transaction_type(df, default_config)
        assert (
            result.collect()[0]["TRANSACTION_TYPE"]
            == "Private"
        )

    def test_empty_claim_type_defaults_to_unknown(
        self, spark, default_config
    ):
        df = _claims_df(spark, [self._row("")])
        result = add_transaction_type(df, default_config)
        assert (
            result.collect()[0]["TRANSACTION_TYPE"]
            == "Unknown"
        )

    def test_null_claim_type_defaults_to_unknown(
        self, spark, default_config
    ):
        df = _claims_df(spark, [self._row(None)])
        result = add_transaction_type(df, default_config)
        assert (
            result.collect()[0]["TRANSACTION_TYPE"]
            == "Unknown"
        )

    def test_unknown_claim_type_defaults(
        self, spark, default_config
    ):
        df = _claims_df(spark, [self._row("99")])
        result = add_transaction_type(df, default_config)
        assert (
            result.collect()[0]["TRANSACTION_TYPE"]
            == "Unknown"
        )

    def test_custom_mapping_from_config(self, spark):
        """Verify mapping is config-driven, not hardcoded."""
        custom_config = {
            "transaction_type_mapping": {
                "3": "Government",
                "4": "NGO",
            },
            "transaction_type_default": "Unclassified",
        }
        df = _claims_df(
            spark,
            [
                self._row("3"),
                self._row("4"),
                self._row("1"),
            ],
        )
        result = add_transaction_type(
            df, custom_config
        ).collect()
        assert result[0]["TRANSACTION_TYPE"] == "Government"
        assert result[1]["TRANSACTION_TYPE"] == "NGO"
        # "1" not in custom mapping
        assert (
            result[2]["TRANSACTION_TYPE"] == "Unclassified"
        )

    def test_empty_mapping_uses_default(self, spark):
        cfg = {
            "transaction_type_mapping": {},
            "transaction_type_default": "Fallback",
        }
        df = _claims_df(spark, [self._row("2")])
        result = add_transaction_type(df, cfg)
        assert (
            result.collect()[0]["TRANSACTION_TYPE"]
            == "Fallback"
        )


# -- add_transaction_direction ---------------------------------


class TestAddTransactionDirection:
    def _row(self, claim_id):
        return (
            claim_id,
            "S",
            "1",
            "1",
            "01.01.2020",
            "10",
            "01.01.2020 10:00",
        )

    def test_cl_prefix(self, spark, default_config):
        df = _claims_df(spark, [self._row("CL_123")])
        result = add_transaction_direction(
            df, default_config
        )
        assert (
            result.collect()[0]["TRANSACTION_DIRECTION"]
            == "COINSURANCE"
        )

    def test_rx_prefix(self, spark, default_config):
        df = _claims_df(spark, [self._row("RX_456")])
        result = add_transaction_direction(
            df, default_config
        )
        assert (
            result.collect()[0]["TRANSACTION_DIRECTION"]
            == "REINSURANCE"
        )

    def test_cx_prefix_returns_null(
        self, spark, default_config
    ):
        df = _claims_df(spark, [self._row("CX_789")])
        result = add_transaction_direction(
            df, default_config
        )
        assert (
            result.collect()[0]["TRANSACTION_DIRECTION"]
            is None
        )

    def test_u_prefix_returns_null(
        self, spark, default_config
    ):
        df = _claims_df(spark, [self._row("U_111")])
        result = add_transaction_direction(
            df, default_config
        )
        assert (
            result.collect()[0]["TRANSACTION_DIRECTION"]
            is None
        )

    def test_custom_direction_from_config(self, spark):
        cfg = {
            "transaction_direction_mapping": {
                "XX": "FRONTING"
            },
            "transaction_type_mapping": {},
            "transaction_type_default": "X",
        }
        df = _claims_df(spark, [self._row("XX_1")])
        result = add_transaction_direction(df, cfg)
        assert (
            result.collect()[0]["TRANSACTION_DIRECTION"]
            == "FRONTING"
        )


# -- add_conformed_value ---------------------------------------


class TestAddConformedValue:
    def _row(self, amount):
        return (
            "CL_1",
            "S",
            "1",
            "1",
            "01.01.2020",
            amount,
            "01.01.2020 10:00",
        )

    def test_decimal_cast(self, spark):
        df = _claims_df(spark, [self._row("523.21")])
        result = add_conformed_value(df)
        val = result.collect()[0]["CONFORMED_VALUE"]
        assert val == Decimal("523.21000")

    def test_integer_amount(self, spark):
        df = _claims_df(spark, [self._row("98465")])
        result = add_conformed_value(df)
        val = result.collect()[0]["CONFORMED_VALUE"]
        assert val == Decimal("98465.00000")

    def test_null_amount(self, spark):
        df = _claims_df(spark, [self._row(None)])
        result = add_conformed_value(df)
        assert result.collect()[0]["CONFORMED_VALUE"] is None


# -- add_business_date -----------------------------------------


class TestAddBusinessDate:
    def test_parses_dd_mm_yyyy(self, spark):
        row = (
            "CL_1",
            "S",
            "1",
            "1",
            "14.02.2021",
            "10",
            "01.01.2020 10:00",
        )
        df = _claims_df(spark, [row])
        result = add_business_date(df, "dd.MM.yyyy")
        assert (
            result.collect()[0]["BUSINESS_DATE"]
            == date(2021, 2, 14)
        )

    def test_null_date(self, spark):
        row = (
            "CL_1",
            "S",
            "1",
            "1",
            None,
            "10",
            "01.01.2020 10:00",
        )
        df = _claims_df(spark, [row])
        result = add_business_date(df, "dd.MM.yyyy")
        assert (
            result.collect()[0]["BUSINESS_DATE"] is None
        )


# -- add_creation_date -----------------------------------------


class TestAddCreationDate:
    def test_parses_datetime(self, spark):
        row = (
            "CL_1",
            "S",
            "1",
            "1",
            "01.01.2020",
            "10",
            "17.01.2022 14:45",
        )
        df = _claims_df(spark, [row])
        result = add_creation_date(df, "dd.MM.yyyy HH:mm")
        ts = result.collect()[0]["CREATION_DATE"]
        assert ts == datetime(2022, 1, 17, 14, 45)

    def test_null_creation_date(self, spark):
        row = (
            "CL_1",
            "S",
            "1",
            "1",
            "01.01.2020",
            "10",
            None,
        )
        df = _claims_df(spark, [row])
        result = add_creation_date(df, "dd.MM.yyyy HH:mm")
        assert (
            result.collect()[0]["CREATION_DATE"] is None
        )


# -- add_system_timestamp --------------------------------------


class TestAddSystemTimestamp:
    def test_not_null(self, spark):
        df = _claims_df(spark, [_ROW])
        result = add_system_timestamp(df)
        assert (
            result.collect()[0]["SYSTEM_TIMESTAMP"]
            is not None
        )


# -- add_nse_id ------------------------------------------------


class TestAddNseId:
    def test_applies_hash_fn(self, spark):
        df = _claims_df(spark, [_ROW])
        result = add_nse_id(
            df, lambda cid: f"hash_{cid}"
        )
        assert result.collect()[0]["NSE_ID"] == "hash_CL_1"

    def test_null_claim_id(self, spark):
        row = (
            None,
            "S",
            "1",
            "1",
            "01.01.2020",
            "10",
            "01.01.2020 10:00",
        )
        df = _claims_df(spark, [row])
        result = add_nse_id(
            df,
            lambda cid: None if cid is None else f"h_{cid}",
        )
        assert result.collect()[0]["NSE_ID"] is None


# -- add_contract_source_system_id (join) ----------------------


class TestAddContractSourceSystemId:
    def test_matching_join(self, spark, default_config):
        claims = _claims_df(
            spark,
            [
                (
                    "CL_1",
                    "SYS_A",
                    "100",
                    "1",
                    "01.01.2020",
                    "10",
                    "01.01.2020 10:00",
                )
            ],
        )
        contracts = _contracts_df(
            spark, [("SYS_A", "100")]
        )
        result = add_contract_source_system_id(
            claims, contracts, default_config
        )
        assert (
            result.collect()[0][
                "CONTRACT_SOURCE_SYSTEM_ID"
            ]
            == 100
        )

    def test_non_matching_join_returns_null(
        self, spark, default_config
    ):
        claims = _claims_df(
            spark,
            [
                (
                    "CL_1",
                    "SYS_A",
                    "999",
                    "1",
                    "01.01.2020",
                    "10",
                    "01.01.2020 10:00",
                )
            ],
        )
        contracts = _contracts_df(
            spark, [("SYS_A", "100")]
        )
        result = add_contract_source_system_id(
            claims, contracts, default_config
        )
        assert (
            result.collect()[0][
                "CONTRACT_SOURCE_SYSTEM_ID"
            ]
            is None
        )

    def test_different_source_system_no_match(
        self, spark, default_config
    ):
        claims = _claims_df(
            spark,
            [
                (
                    "CL_1",
                    "SYS_B",
                    "100",
                    "1",
                    "01.01.2020",
                    "10",
                    "01.01.2020 10:00",
                )
            ],
        )
        contracts = _contracts_df(
            spark, [("SYS_A", "100")]
        )
        result = add_contract_source_system_id(
            claims, contracts, default_config
        )
        assert (
            result.collect()[0][
                "CONTRACT_SOURCE_SYSTEM_ID"
            ]
            is None
        )


# -- build_transactions (end-to-end) --------------------------


class TestBuildTransactions:
    def test_output_columns(self, spark, default_config):
        claims = _claims_df(
            spark,
            [
                (
                    "CL_123",
                    "SYS",
                    "1",
                    "2",
                    "14.02.2021",
                    "100",
                    "17.01.2022 14:45",
                )
            ],
        )
        contracts = _contracts_df(spark, [("SYS", "1")])
        result = build_transactions(
            claims,
            contracts,
            default_config,
            lambda c: "abc123",
        )
        expected_cols = [
            "CONTRACT_SOURCE_SYSTEM",
            "CONTRACT_SOURCE_SYSTEM_ID",
            "SOURCE_SYSTEM_ID",
            "TRANSACTION_TYPE",
            "TRANSACTION_DIRECTION",
            "CONFORMED_VALUE",
            "BUSINESS_DATE",
            "CREATION_DATE",
            "SYSTEM_TIMESTAMP",
            "NSE_ID",
        ]
        assert result.columns == expected_cols

    def test_single_row_values(
        self, spark, default_config
    ):
        claims = _claims_df(
            spark,
            [
                (
                    "CL_123",
                    "SYS",
                    "1",
                    "2",
                    "14.02.2021",
                    "100.5",
                    "17.01.2022 14:45",
                )
            ],
        )
        contracts = _contracts_df(spark, [("SYS", "1")])
        result = build_transactions(
            claims,
            contracts,
            default_config,
            lambda c: "deadbeef",
        )
        row = result.collect()[0]
        assert row["CONTRACT_SOURCE_SYSTEM"] == "Europe 3"
        assert row["SOURCE_SYSTEM_ID"] == 123
        assert row["TRANSACTION_TYPE"] == "Corporate"
        assert (
            row["TRANSACTION_DIRECTION"] == "COINSURANCE"
        )
        assert row["NSE_ID"] == "deadbeef"
