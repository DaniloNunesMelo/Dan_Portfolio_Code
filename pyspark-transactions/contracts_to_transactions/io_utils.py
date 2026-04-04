"""CSV read / write helpers."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

def read_csv(
    spark: SparkSession,
    path: str,
    *,
    header: bool = True,
    infer_schema: bool = False,
    delimiter: str = ",",
) -> DataFrame:
    """Read a CSV file into a Spark DataFrame.

    Parameters
    ----------
    spark : SparkSession
    path : str
        Path to the CSV file.
    header : bool
        Whether the first row is a header.
    infer_schema : bool
        Let Spark infer column types (default False -> strings).
    delimiter : str
        Column delimiter.

    Returns
    -------
    DataFrame

    Raises
    ------
    FileNotFoundError
        If *path* does not exist on the local filesystem.
    """
    if not Path(path).exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    logger.info("Reading CSV: %s", path)
    return (
        spark.read.option("header", header)
        .option("inferSchema", infer_schema)
        .option("delimiter", delimiter)
        .csv(path)
    )


def write_csv(
    df: DataFrame,
    path: str,
    *,
    header: bool = True,
    delimiter: str = ",",
    mode: str = "overwrite",
) -> None:
    """Write a DataFrame as a single CSV file.

    Spark normally writes a directory of part files.  This helper
    coalesces to 1 partition so the user gets a single file, then
    renames it to the requested *path*.

    Parameters
    ----------
    df : DataFrame
    path : str
        Desired output file path.
    header : bool
    delimiter : str
    mode : str
        Spark write mode (``overwrite`` | ``append`` | ``error``).
    """
    out = Path(path)
    tmp_dir = out.parent / f".tmp_{out.stem}"

    logger.info("Writing CSV: %s (via temp dir %s)", path, tmp_dir)
    (
        df.coalesce(1)
        .write.option("header", header)
        .option("delimiter", delimiter)
        .mode(mode)
        .csv(str(tmp_dir))
    )

    # Find the single part-* file and move it
    part_files = list(tmp_dir.glob("part-*"))
    if not part_files:
        import os

        contents = os.listdir(tmp_dir) if tmp_dir.exists() else "DIRECTORY_NOT_FOUND"
        raise RuntimeError(
            f"No part files found in {tmp_dir}. This indicates a Spark write failure. "
            f"Directory contents: {contents}. Possible causes: DataFrame is empty, "
            f"insufficient disk space, or invalid Spark config. "
            f"Verify input data is not empty and check available disk space."
        )

    part_files[0].rename(out)

    # Clean up temp directory
    shutil.rmtree(tmp_dir, ignore_errors=True)
    logger.info("Output written: %s", out)
