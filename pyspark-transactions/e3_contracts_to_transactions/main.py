"""CLI entrypoint: wire I/O, API, and Spark transformations together.

Orchestration only — all business logic lives in transform.py.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from typing import Callable, List, Tuple

from pyspark.sql import SparkSession

from .api import fetch_nse_id
from .io_utils import read_claims, read_contracts, write_single_csv
from .schemas import NSE_LOOKUP_SCHEMA
from .transform import build_transactions

logger = logging.getLogger(__name__)


def get_spark(app_name: str = "transactions_task") -> SparkSession:
    """Return the active SparkSession or create a local one for dev.

    Spark 4.0 requires Java 17 or 21.
    """
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing

    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        raise RuntimeError(
            "No active SparkSession on Databricks. "
            "Run from a notebook attached to a cluster."
        )

    # Pre-flight: check that Java is reachable
    import shutil

    java_home = os.environ.get("JAVA_HOME")
    java_bin = shutil.which("java")

    if not java_bin and not java_home:
        raise RuntimeError(
            "Java not found. PySpark 4.0 requires Java 17 or 21.\n"
            "Install a JDK and either:\n"
            "  • add it to PATH, or\n"
            "  • set JAVA_HOME (e.g. export JAVA_HOME=/usr/lib/jvm/java-17-openjdk)\n"
            "\n"
            "macOS:   brew install openjdk@17\n"
            "Ubuntu:  sudo apt install openjdk-17-jdk\n"
            "Windows: download from https://adoptium.net"
        )

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build TRANSACTIONS from CONTRACT + CLAIM CSVs"
    )
    parser.add_argument("--contracts", required=True, help="Path to CONTRACT CSV")
    parser.add_argument("--claims", required=True, help="Path to CLAIM CSV")
    parser.add_argument(
        "--output",
        default="output/TRANSACTIONS.csv",
        help="Single-CSV output path",
    )
    return parser.parse_args(argv)


def build_nse_lookup_pairs(
    claims_csv_path: str,
    hash_fn: Callable[[str], str] = fetch_nse_id,
) -> List[Tuple[str, str]]:
    """Read distinct CLAIM_IDs from the CSV and compute NSE_ID for each.

    Parameters
    ----------
    claims_csv_path:
        Filesystem path to the CLAIM CSV.
    hash_fn:
        A callable ``(claim_id) -> nse_id``. Defaults to the real API call.
        Tests inject a deterministic stub.
    """
    claim_ids: list[str] = []
    seen: set[str] = set()

    with open(claims_csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if "CLAIM_ID" not in (reader.fieldnames or []):
            raise ValueError("CLAIM CSV must contain a CLAIM_ID column")
        for row in reader:
            cid = (row.get("CLAIM_ID") or "").strip()
            if cid and cid not in seen:
                seen.add(cid)
                claim_ids.append(cid)

    logger.info("Computing NSE_ID for %d distinct claim IDs", len(claim_ids))
    return [(cid, hash_fn(cid)) for cid in claim_ids]


def run(argv: list[str]) -> int:
    """Main pipeline: read -> enrich -> transform -> write."""
    args = _parse_args(argv)

    pairs = build_nse_lookup_pairs(args.claims)

    spark = get_spark()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "1")

    try:
        contracts_df = read_contracts(spark, args.contracts)
        claims_df = read_claims(spark, args.claims)
        nse_lookup_df = spark.createDataFrame(pairs, schema=NSE_LOOKUP_SCHEMA)

        tx = build_transactions(contracts_df, claims_df, nse_lookup_df=nse_lookup_df)
        write_single_csv(tx, args.output)

        logger.info("Pipeline complete -> %s", args.output)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    raise SystemExit(run(sys.argv[1:]))
