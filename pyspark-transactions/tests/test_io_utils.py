"""Tests for io_utils.py -- CSV read / write helpers.

These tests target the previously uncovered lines 19-20, 25-26,
35-54 from the coverage report.
"""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from e3_contracts_to_transactions.io_utils import (
    read_csv,
    write_csv,
)


# -- read_csv --------------------------------------------------


class TestReadCsv:
    def test_reads_valid_csv(self, spark, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "NAME,AGE\nAlice,30\nBob,25\n",
            encoding="utf-8",
        )

        df = read_csv(spark, str(csv_file))
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["NAME"] == "Alice"
        assert rows[1]["AGE"] == "25"

    def test_reads_with_infer_schema(self, spark, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "NAME,AGE\nAlice,30\nBob,25\n",
            encoding="utf-8",
        )

        df = read_csv(
            spark, str(csv_file), infer_schema=True
        )
        rows = df.collect()
        assert rows[0]["AGE"] == 30

    def test_reads_with_custom_delimiter(
        self, spark, tmp_path
    ):
        csv_file = tmp_path / "test.tsv"
        csv_file.write_text(
            "NAME\tAGE\nAlice\t30\n", encoding="utf-8"
        )

        df = read_csv(
            spark, str(csv_file), delimiter="\t"
        )
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["NAME"] == "Alice"

    def test_reads_without_header(self, spark, tmp_path):
        csv_file = tmp_path / "noheader.csv"
        csv_file.write_text(
            "Alice,30\nBob,25\n", encoding="utf-8"
        )

        df = read_csv(spark, str(csv_file), header=False)
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["_c0"] == "Alice"

    def test_file_not_found_raises(self, spark, tmp_path):
        with pytest.raises(
            FileNotFoundError, match="Input file not found"
        ):
            read_csv(
                spark, str(tmp_path / "nonexistent.csv")
            )

    def test_empty_csv_returns_empty_df(
        self, spark, tmp_path
    ):
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text(
            "NAME,AGE\n", encoding="utf-8"
        )

        df = read_csv(spark, str(csv_file))
        assert df.count() == 0
        assert "NAME" in df.columns

    def test_csv_with_quoted_fields(self, spark, tmp_path):
        csv_file = tmp_path / "quoted.csv"
        csv_file.write_text(
            'NAME,DESC\nAlice,"Has, comma"\n',
            encoding="utf-8",
        )

        df = read_csv(spark, str(csv_file))
        row = df.collect()[0]
        assert row["DESC"] == "Has, comma"


# -- write_csv -------------------------------------------------


class TestWriteCsv:
    def _make_df(self, spark: SparkSession) -> DataFrame:
        schema = StructType(
            [
                StructField("A", StringType()),
                StructField("B", StringType()),
            ]
        )
        return spark.createDataFrame(
            [("x", "1"), ("y", "2")], schema
        )

    def test_writes_single_csv_file(self, spark, tmp_path):
        out_path = tmp_path / "output.csv"
        df = self._make_df(spark)

        write_csv(df, str(out_path))

        assert out_path.exists()
        assert out_path.is_file()
        content = out_path.read_text(encoding="utf-8")
        assert "A,B" in content
        assert "x,1" in content

    def test_temp_dir_is_cleaned_up(self, spark, tmp_path):
        out_path = tmp_path / "output.csv"
        df = self._make_df(spark)

        write_csv(df, str(out_path))

        tmp_dir = tmp_path / ".tmp_output"
        assert not tmp_dir.exists()

    def test_writes_without_header(self, spark, tmp_path):
        out_path = tmp_path / "no_header.csv"
        df = self._make_df(spark)

        write_csv(df, str(out_path), header=False)

        content = out_path.read_text(encoding="utf-8")
        assert "A,B" not in content
        assert "x,1" in content

    def test_writes_with_custom_delimiter(
        self, spark, tmp_path
    ):
        out_path = tmp_path / "output.tsv"
        df = self._make_df(spark)

        write_csv(df, str(out_path), delimiter="\t")

        content = out_path.read_text(encoding="utf-8")
        assert "A\tB" in content
        assert "x\t1" in content

    def test_overwrite_mode(self, spark, tmp_path):
        out_path = tmp_path / "output.csv"
        df = self._make_df(spark)

        write_csv(df, str(out_path))
        write_csv(df, str(out_path), mode="overwrite")

        assert out_path.exists()

    def test_output_parent_dir_can_exist(
        self, spark, tmp_path
    ):
        """Ensure writing works when parent exists."""
        sub = tmp_path / "subdir"
        sub.mkdir()
        out_path = sub / "output.csv"
        df = self._make_df(spark)

        write_csv(df, str(out_path))
        assert out_path.exists()

    def test_multi_partition_coalesced_to_one(
        self, spark, tmp_path
    ):
        """Even a repartitioned DF produces one file."""
        out_path = tmp_path / "output.csv"
        df = self._make_df(spark).repartition(4)

        write_csv(df, str(out_path))
        assert out_path.is_file()
