"""Pure Spark transformations -- no I/O, no side-effects.

Every public function takes a DataFrame (and optionally config)
and returns a new DataFrame.  This keeps transforms testable in
isolation.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType, StringType

logger = logging.getLogger(__name__)


def add_source_system(
    df: DataFrame,
    source_system: str,
) -> DataFrame:
    """Add literal CONTRACT_SOURCE_SYSTEM column."""
    return df.withColumn("CONTRACT_SOURCE_SYSTEM", F.lit(source_system))


def add_contract_source_system_id(
    claims_df: DataFrame,
    contracts_df: DataFrame,
    config: dict[str, Any],
) -> DataFrame:
    """Left-join claims to contracts and carry over CONTRACT_ID.

    Uses join key column names from *config["claim_contract_join"]*.
    Logs the number of matched and unmatched claims.
    """
    join_cfg = config.get("claim_contract_join", {})
    claim_cid = join_cfg.get("claim_contract_id_col", "CONTRACT_ID")
    claim_ss = join_cfg.get("claim_source_system_col", "CONTRACT_SOURCE_SYSTEM")
    contract_cid = join_cfg.get("contract_id_col", "CONTRACT_ID")
    contract_ss = join_cfg.get("contract_source_system_col", "SOURCE_SYSTEM")

    contracts_subset = contracts_df.select(
        F.col(contract_ss).alias("_ctr_ss"),
        F.col(contract_cid).alias("_ctr_id"),
    )

    joined = claims_df.join(
        contracts_subset,
        on=(
            (claims_df[claim_ss] == contracts_subset["_ctr_ss"])
            & (claims_df[claim_cid] == contracts_subset["_ctr_id"])
        ),
        how="left",
    )

    result = joined.withColumn(
        "CONTRACT_SOURCE_SYSTEM_ID",
        F.when(
            F.col("_ctr_id").isNotNull(),
            F.col(claim_cid),
        ).cast("long"),
    ).drop("_ctr_ss", "_ctr_id")

    # Log join statistics
    matched = result.filter(F.col("CONTRACT_SOURCE_SYSTEM_ID").isNotNull()).count()
    unmatched = result.filter(F.col("CONTRACT_SOURCE_SYSTEM_ID").isNull()).count()
    logger.info(f"Join complete: {matched} matched, {unmatched} unmatched claims")

    return result


def add_source_system_id(df: DataFrame) -> DataFrame:
    """Extract numeric suffix from CLAIM_ID.

    Example: ``CL_68545123`` -> ``68545123``.
    Returns NULL if CLAIM_ID does not contain underscore and digits.
    """
    extracted = F.regexp_extract(F.col("CLAIM_ID"), r"_(\d+)$", 1)
    return df.withColumn(
        "SOURCE_SYSTEM_ID",
        F.when(extracted == "", F.lit(None)).otherwise(extracted.cast("int")),
    )


def add_transaction_type(
    df: DataFrame,
    config: dict[str, Any],
) -> DataFrame:
    """Map CLAIM_TYPE to TRANSACTION_TYPE using the config."""
    mapping = config["transaction_type_mapping"]
    default = config["transaction_type_default"]

    # Build a chained when/otherwise from the mapping dict
    expr = None
    for claim_val, label in mapping.items():
        condition = F.col("CLAIM_TYPE") == str(claim_val)
        if expr is None:
            expr = F.when(condition, F.lit(label))
        else:
            expr = expr.when(condition, F.lit(label))

    if expr is None:
        col_expr = F.lit(default)
    else:
        col_expr = expr.otherwise(F.lit(default))

    return df.withColumn("TRANSACTION_TYPE", col_expr)


def add_transaction_direction(
    df: DataFrame,
    config: dict[str, Any],
) -> DataFrame:
    """Derive TRANSACTION_DIRECTION from CLAIM_ID prefix."""
    mapping = config["transaction_direction_mapping"]

    expr = None
    for prefix, direction in mapping.items():
        condition = F.col("CLAIM_ID").startswith(f"{prefix}_")
        if expr is None:
            expr = F.when(condition, F.lit(direction))
        else:
            expr = expr.when(condition, F.lit(direction))

    if expr is not None:
        col_expr = expr.otherwise(F.lit(None).cast(StringType()))
    else:
        col_expr = F.lit(None).cast(StringType())

    return df.withColumn("TRANSACTION_DIRECTION", col_expr)


def add_conformed_value(df: DataFrame) -> DataFrame:
    """Cast AMOUNT to decimal(16,5)."""
    return df.withColumn(
        "CONFORMED_VALUE",
        F.col("AMOUNT").cast(DecimalType(16, 5)),
    )


def add_business_date(
    df: DataFrame,
    date_fmt: str,
) -> DataFrame:
    """Parse DATE_OF_LOSS into a date column."""
    return df.withColumn(
        "BUSINESS_DATE",
        F.to_date(F.col("DATE_OF_LOSS"), date_fmt),
    )


def add_creation_date(
    df: DataFrame,
    datetime_fmt: str,
) -> DataFrame:
    """Parse CREATION_DATE (string) into a timestamp column."""
    return df.withColumn(
        "CREATION_DATE",
        F.to_timestamp(F.col("CREATION_DATE"), datetime_fmt),
    )


def add_system_timestamp(df: DataFrame) -> DataFrame:
    """Add current_timestamp()."""
    return df.withColumn("SYSTEM_TIMESTAMP", F.current_timestamp())


def add_transaction_category(
    df: DataFrame,
    config: dict[str, Any],
) -> DataFrame:
    """Categorize transactions based on CONFORMED_VALUE sign.

    Negative values indicate refunds or recoveries; positive values are charges.
    Labels are driven by config keys: ``transaction_category_negative`` and
    ``transaction_category_positive``.
    """
    negative_label = config.get("transaction_category_negative", "REFUND")
    positive_label = config.get("transaction_category_positive", "CHARGE")

    return df.withColumn(
        "TRANSACTION_CATEGORY",
        F.when(F.col("CONFORMED_VALUE") < 0, F.lit(negative_label)).otherwise(
            F.lit(positive_label)
        ),
    )


def add_nse_id(
    df: DataFrame,
    hash_fn: Callable[[str | None], str | None],
) -> DataFrame:
    """Compute NSE_ID via a UDF that calls *hash_fn*."""
    hash_udf = F.udf(hash_fn, StringType())
    return df.withColumn("NSE_ID", hash_udf(F.col("CLAIM_ID")))


def build_transactions(
    claims_df: DataFrame,
    contracts_df: DataFrame,
    config: dict[str, Any],
    hash_fn: Callable[[str | None], str | None],
) -> DataFrame:
    """Apply every transform step and select final columns.

    Logs the number of input and output rows for observability.
    """
    from .schemas import TRANSACTIONS_SCHEMA

    initial_count = claims_df.count()
    logger.info(f"Starting transformation with {initial_count} input claims")

    df = add_contract_source_system_id(claims_df, contracts_df, config)
    df = add_source_system(df, config["source_system"])
    df = add_source_system_id(df)
    df = add_transaction_type(df, config)
    df = add_transaction_direction(df, config)
    df = add_conformed_value(df)
    df = add_business_date(df, config["date_of_loss_format"])
    df = add_creation_date(df, config["creation_date_format"])
    df = add_system_timestamp(df)
    df = add_nse_id(df, hash_fn)
    df = add_transaction_category(df, config)
    logger.debug("All transformations complete")

    target_cols = [f.name for f in TRANSACTIONS_SCHEMA.fields]
    result = df.select(*target_cols)

    final_count = result.count()
    logger.info(
        f"Transformation complete: {initial_count} input → {final_count} output "
        f"(reduction: {initial_count - final_count})"
    )

    return result

