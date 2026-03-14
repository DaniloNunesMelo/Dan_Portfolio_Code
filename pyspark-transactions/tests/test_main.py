"""Tests for the main orchestration module.

Covers build_nse_lookup_pairs with an injected stub hash function
(no HTTP calls, no Spark).
"""

from __future__ import annotations
import os
import pytest
from e3_contracts_to_transactions.main import build_nse_lookup_pairs

class TestBuildNseLookupPairs:

    def _write_csv(self, tmp_dir: str, content: str) -> str:
        path = os.path.join(tmp_dir, "claims.csv")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content)
        return path

    def test_returns_pairs_for_each_unique_claim_id(self, tmp_path):
        csv_content = (
            "SOURCE_SYSTEM,CLAIM_ID,CONTRACT_SOURCE_SYSTEM,CONTRACT_ID,"
            "CLAIM_TYPE,DATE_OF_LOSS,AMOUNT,CREATION_DATE\n"
            "X,CL_1,Y,1,1,01.01.2020,10,01.01.2020 10:00\n"
            "X,CL_2,Y,1,2,01.01.2020,20,01.01.2020 10:00\n"
            "X,CL_1,Y,1,1,01.01.2020,10,01.01.2020 10:00\n"  # duplicate
        )
        path = self._write_csv(str(tmp_path), csv_content)
        stub_hash = lambda cid: f"hash_{cid}"  # noqa: E731

        pairs = build_nse_lookup_pairs(path, hash_fn=stub_hash)

        assert pairs == [("CL_1", "hash_CL_1"), ("CL_2", "hash_CL_2")]

    def test_raises_on_missing_claim_id_column(self, tmp_path):
        csv_content = "SOURCE_SYSTEM,NOT_CLAIM_ID\nX,Y\n"
        path = self._write_csv(str(tmp_path), csv_content)

        with pytest.raises(ValueError, match="CLAIM_ID"):
            build_nse_lookup_pairs(path, hash_fn=lambda x: x)

    def test_skips_empty_claim_ids(self, tmp_path):
        csv_content = (
            "SOURCE_SYSTEM,CLAIM_ID,CONTRACT_SOURCE_SYSTEM,CONTRACT_ID,"
            "CLAIM_TYPE,DATE_OF_LOSS,AMOUNT,CREATION_DATE\n"
            "X,,Y,1,1,01.01.2020,10,01.01.2020 10:00\n"
            "X,CL_5,Y,1,2,01.01.2020,20,01.01.2020 10:00\n"
        )
        path = self._write_csv(str(tmp_path), csv_content)

        pairs = build_nse_lookup_pairs(path, hash_fn=lambda cid: f"h_{cid}")

        assert len(pairs) == 1
        assert pairs[0] == ("CL_5", "h_CL_5")
