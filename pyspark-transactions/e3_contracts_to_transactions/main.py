"""CLI entrypoint & orchestration for the Europe 3 pipeline."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from .api import make_hashify_fn
from .config import load_parameters
from .io_utils import read_csv, write_csv
from .transform import build_transactions

logger = logging.getLogger(__name__)


def validate_nse_ids(df: DataFrame, allow_null: bool = False) -> DataFrame:
    """Validate that NSE_ID column meets requirements.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with NSE_ID column.
    allow_null : bool, default False
        If False, raises error if any NSE_ID is null.

    Returns
    -------
    DataFrame
        Same DataFrame (unmodified).

    Raises
    ------
    ValueError
        If allow_null=False and null NSE_IDs exist.
    """
    null_count = df.filter(F.col("NSE_ID").isNull()).count()
    total_count = df.count()

    if not allow_null and null_count > 0:
        logger.error(
            f"Data quality check failed: {null_count} NULL NSE_IDs out of {total_count} rows. "
            f"This indicates API failures during hashing. Review logs and retry."
        )
        raise ValueError(
            f"NSE_ID validation failed: {null_count}/{total_count} rows have NULL NSE_ID. "
            f"This indicates Hashify API failures. Check API connectivity and retry."
        )

    if null_count > 0:
        logger.warning(f"Allowing {null_count} NULL NSE_IDs (allow_null={allow_null})")

    return df


def parse_args(
    argv: list[str] | None = None,
) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description=("Europe 3 — Contracts-to-Transactions pipeline"),
    )
    parser.add_argument(
        "--contracts",
        required=True,
        help="Path to the contracts CSV file",
    )
    parser.add_argument(
        "--claims",
        required=True,
        help="Path to the claims CSV file",
    )
    parser.add_argument(
        "--output",
        default="output/TRANSACTIONS.csv",
        help=(
            "Path for the output TRANSACTIONS CSV (default: output/TRANSACTIONS.csv)"
        ),
    )
    parser.add_argument(
        "--config",
        default=None,
        help=("Path to parameters.yaml (default: config/parameters.yaml)"),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    return parser.parse_args(argv)


def create_spark_session(
    app_name: str = "Europe3_Pipeline",
) -> SparkSession:
    """Build or retrieve a SparkSession."""
    return SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()


def run_pipeline(
    spark: SparkSession,
    config: dict[str, Any],
    contracts_path: str,
    claims_path: str,
    output_path: str,
) -> None:
    """Execute the full pipeline: read -> transform -> validate -> write."""
    contracts_df = read_csv(spark, contracts_path)
    claims_df = read_csv(spark, claims_path)

    hash_fn = make_hashify_fn(
        base_url=config.get(
            "hashify_base_url",
            "https://api.hashify.net/hash/md4/hex",
        ),
        response_field=config.get("hashify_response_field", "Digest"),
        max_retries=config.get("hashify_max_retries", 3),
    )

    transactions_df = build_transactions(claims_df, contracts_df, config, hash_fn)

    # Validate data quality before writing
    validate_nse_ids(transactions_df, allow_null=False)

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    write_csv(
        transactions_df,
        output_path,
        header=config.get("output_header", True),
        delimiter=config.get("output_delimiter", ","),
    )
    logger.info(
        "Pipeline finished. %d transactions written.",
        transactions_df.count(),
    )


def main(argv: list[str] | None = None) -> None:
    """Main entry point -- parse args, load config, run."""
    args = parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format=("%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"),
    )

    config = load_parameters(args.config)
    logger.info(
        "Config loaded: source_system=%s",
        config["source_system"],
    )

    spark = create_spark_session()
    try:
        run_pipeline(
            spark,
            config,
            args.contracts,
            args.claims,
            args.output,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
