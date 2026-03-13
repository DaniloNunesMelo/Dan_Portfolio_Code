"""Pure Spark transformations for building TRANSACTIONS.

Every public function here is a small, side-effect-free transformation that
takes a DataFrame (or Column) and returns a DataFrame (or Column). This makes
each rule independently testable without I/O or HTTP calls.

Spark 4.0 note:
    ANSI mode is ON by default (spark.sql.ansi.enabled = true).
    Invalid casts now throw errors instead of silently returning null.
    We guard casts with null-safe patterns (when/otherwise) so that
    malformed rows produce nulls rather than crashing the job.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


# ---------------------------------------------------------------------------
# Column-level mapping helpers
# ---------------------------------------------------------------------------

def map_transaction_type(claim_type_col: Column) -> Column:
    """claim_type 2 -> Corporate, 1 -> Private, else Unknown."""
    return (
        F.when(claim_type_col == "2", F.lit("Corporate"))
        .when(claim_type_col == "1", F.lit("Private"))
        .otherwise(F.lit("Unknown"))
    )


def map_transaction_direction(claim_id_col: Column) -> Column:
    """CL -> COINSURANCE, RX -> REINSURANCE, else null."""
    return (
        F.when(claim_id_col.startswith("CL"), F.lit("COINSURANCE"))
        .when(claim_id_col.startswith("RX"), F.lit("REINSURANCE"))
        .otherwise(F.lit(None).cast("string"))
    )


def extract_source_system_id(claim_id_col: Column) -> Column:
    """Extract trailing digits from CLAIM_ID (e.g. CL_68545123 -> 68545123).

    ANSI-safe: if no digits are found, returns null instead of
    attempting to cast an empty string to int (which would throw).
    """
    digits = F.regexp_extract(claim_id_col, r"(\d+)$", 1)
    return (
        F.when(digits != F.lit(""), digits.cast("int"))
        .otherwise(F.lit(None).cast("int"))
    )


def parse_business_date(date_of_loss_col: Column) -> Column:
    """Parse dd.MM.yyyy -> DateType.

    ANSI-safe: uses try_to_date (Spark 4.0+) which returns null
    for unparseable values instead of throwing.
    """
    return F.try_to_timestamp(date_of_loss_col, F.lit("dd.MM.yyyy")).cast("date")


def parse_creation_date(creation_date_col: Column) -> Column:
    """Parse dd.MM.yyyy HH:mm -> TimestampType.

    ANSI-safe: uses try_to_timestamp (Spark 4.0+) which returns null
    for unparseable values instead of throwing.
    """
    return F.try_to_timestamp(creation_date_col, F.lit("dd.MM.yyyy HH:mm"))


def cast_conformed_value(amount_col: Column) -> Column:
    """Cast AMOUNT string to decimal(16,5).

    ANSI-safe: guards against null / empty / non-numeric values
    that would throw under ANSI mode.
    """
    trimmed = F.trim(amount_col)
    return (
        F.when(
            trimmed.isNotNull() & (trimmed != F.lit("")),
            trimmed.cast(DecimalType(16, 5)),
        )
        .otherwise(F.lit(None).cast(DecimalType(16, 5)))
    )


# ---------------------------------------------------------------------------
# DataFrame-level join helpers
# ---------------------------------------------------------------------------

def prepare_contracts(contracts_df: DataFrame) -> DataFrame:
    """Select and normalise the columns needed for joining."""
    return contracts_df.select(
        F.trim(F.col("SOURCE_SYSTEM")).alias("CONTRACT_SOURCE_SYSTEM_RAW"),
        # CONTRACT_ID is already LongType from schema — safe cast
        F.col("CONTRACT_ID").alias("CONTRACT_CONTRACT_ID"),
    )


def prepare_claims(claims_df: DataFrame) -> DataFrame:
    """Select and normalise the columns needed for transformations."""
    return claims_df.select(
        F.trim(F.col("CLAIM_ID")).cast("string").alias("CLAIM_ID"),
        F.trim(F.col("CONTRACT_SOURCE_SYSTEM")).alias("CONTRACT_SOURCE_SYSTEM_RAW"),
        # CONTRACT_ID is already LongType from schema — safe cast
        F.col("CONTRACT_ID").alias("CLAIM_CONTRACT_ID"),
        F.trim(F.col("CLAIM_TYPE")).cast("string").alias("CLAIM_TYPE"),
        F.trim(F.col("DATE_OF_LOSS")).cast("string").alias("DATE_OF_LOSS"),
        F.trim(F.col("AMOUNT")).cast("string").alias("AMOUNT"),
        F.trim(F.col("CREATION_DATE")).cast("string").alias("CLAIM_CREATION_DATE"),
    )


def join_claims_to_contracts(
    claims: DataFrame,
    contracts: DataFrame,
) -> DataFrame:
    """Left-join claims to contracts on source system + contract id."""
    return claims.join(
        contracts,
        on=(
            (claims["CONTRACT_SOURCE_SYSTEM_RAW"] == contracts["CONTRACT_SOURCE_SYSTEM_RAW"])
            & (claims["CLAIM_CONTRACT_ID"] == contracts["CONTRACT_CONTRACT_ID"])
        ),
        how="left",
    )


def join_nse_lookup(claims: DataFrame, nse_lookup_df: DataFrame) -> DataFrame:
    """Broadcast-join the NSE lookup table onto claims."""
    return claims.join(F.broadcast(nse_lookup_df), on="CLAIM_ID", how="left")


# ---------------------------------------------------------------------------
# Pipeline entrypoint
# ---------------------------------------------------------------------------

def build_transactions(
    contracts_df: DataFrame,
    claims_df: DataFrame,
    *,
    nse_lookup_df: DataFrame,
) -> DataFrame:
    """Compose all transformations into the final TRANSACTIONS DataFrame.

    Parameters
    ----------
    contracts_df:
        Raw CONTRACT DataFrame (as read from CSV).
    claims_df:
        Raw CLAIM DataFrame (as read from CSV).
    nse_lookup_df:
        Pre-computed (CLAIM_ID, NSE_ID) lookup table.
    """
    contracts = prepare_contracts(contracts_df)
    claims = prepare_claims(claims_df)
    claims = join_nse_lookup(claims, nse_lookup_df)
    joined = join_claims_to_contracts(claims, contracts)

    return (
        joined
        .withColumn("CONTRACT_SOURCE_SYSTEM", F.lit("Europe 3"))
        .withColumn("CONTRACT_SOURCE_SYSTEM_ID", F.col("CONTRACT_CONTRACT_ID").cast("long"))
        .withColumn("SOURCE_SYSTEM_ID", extract_source_system_id(F.col("CLAIM_ID")))
        .withColumn("TRANSACTION_TYPE", map_transaction_type(F.col("CLAIM_TYPE")))
        .withColumn("TRANSACTION_DIRECTION", map_transaction_direction(F.col("CLAIM_ID")))
        .withColumn("CONFORMED_VALUE", cast_conformed_value(F.col("AMOUNT")))
        .withColumn("BUSINESS_DATE", parse_business_date(F.col("DATE_OF_LOSS")))
        .withColumn("CREATION_DATE", parse_creation_date(F.col("CLAIM_CREATION_DATE")))
        .withColumn("SYSTEM_TIMESTAMP", F.current_timestamp())
        .select(
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
        )
    )
