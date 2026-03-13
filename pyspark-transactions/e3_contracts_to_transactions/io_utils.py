"""I/O helpers: read input CSVs, write a single output CSV."""

from __future__ import annotations

import glob
import logging
import os
import shutil

from pyspark.sql import DataFrame, SparkSession

from .schemas import CLAIM_SCHEMA, CONTRACT_SCHEMA

logger = logging.getLogger(__name__)


def read_contracts(spark: SparkSession, path: str) -> DataFrame:
    """Read a CONTRACT CSV using the fixed schema."""
    logger.info("Reading contracts from %s", path)
    return spark.read.option("header", True).schema(CONTRACT_SCHEMA).csv(path)


def read_claims(spark: SparkSession, path: str) -> DataFrame:
    """Read a CLAIM CSV using the fixed schema."""
    logger.info("Reading claims from %s", path)
    return spark.read.option("header", True).schema(CLAIM_SCHEMA).csv(path)


def write_single_csv(df: DataFrame, output_path: str) -> None:
    """Coalesce *df* to one partition and write a single CSV with header.

    Spark writes CSV as a directory of part-files. This helper collapses
    them into a single file so the submission is one CSV.
    """
    output_dir = os.path.dirname(os.path.abspath(output_path))
    os.makedirs(output_dir, exist_ok=True)

    tmp_dir = output_path + "__tmp"
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)

    part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if not part_files:
        raise RuntimeError(f"No part-files found in {tmp_dir}")

    if os.path.exists(output_path):
        os.remove(output_path)

    shutil.move(part_files[0], output_path)
    shutil.rmtree(tmp_dir)

    logger.info("Wrote %s", output_path)
