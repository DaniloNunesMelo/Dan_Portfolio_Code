"""Data quality validation for the Europe 3 ETL pipeline."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)


def validate_etl_data(
    df: DataFrame,
    stage: str,
    critical_cols: list[str],
    amount_col: str | None = None,
    id_col: str | None = None,
) -> bool:
    """Validate data quality at each ETL stage.

    Returns True if all checks pass, False otherwise.
    Logs each error found.
    """
    errors = []

    # Check for nulls in critical fields
    for col_name in critical_cols:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            errors.append(f"Found {null_count} nulls in {col_name} at {stage}")

    # Check for negative/invalid amounts
    if amount_col:
        neg_count = df.filter(col(amount_col) < 0).count()
        if neg_count > 0:
            errors.append(f"Found {neg_count} negative values in {amount_col} at {stage}")

    # Check for duplicate IDs
    if id_col:
        total_rows = df.count()
        unique_ids = df.select(id_col).distinct().count()
        if total_rows != unique_ids:
            errors.append(
                f"Found {total_rows - unique_ids} duplicate {id_col} values at {stage}"
            )

    if errors:
        for error in errors:
            logger.error(error)
        return False

    logger.info("✓ Validation passed at stage: %s", stage)
    return True
