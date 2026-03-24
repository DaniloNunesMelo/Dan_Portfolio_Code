"""
tests/test_mt103.py
===================
Unit tests for the MT103 SWIFT parser (mt103.py).

Tests use *synthetic* (hand-crafted) messages so no real payment data is needed.
Each test is self-contained and annotated so the test itself is educational.
"""

import csv
import io
import textwrap
from pathlib import Path

import pytest

# We import from the parent directory; pytest discovers it via conftest.py / sys.path
from mt103 import (
    MT103Message,
    BLOCK_DESCRIPTIONS,
    MT103_FIELD_DESCRIPTIONS,
    describe_block,
    describe_mt103_field,
    enrich_with_descriptions,
    mt103_to_wide_row,
    parse_swift_mt_to_dict,
    read_mt103_batch,
    to_mt103_message,
    write_mt103_wide_csv,
)


# ---------------------------------------------------------------------------
# Synthetic test data
# ---------------------------------------------------------------------------

# A minimal but valid MT103 with the most important fields.
MINIMAL_MT103 = textwrap.dedent("""\
    {1:F01BANKDEFFXXXX0000000001}{2:I103BANKUS33XXXXN}{4:
    :20:TEST-REF-001
    :23B:CRED
    :32A:250115EUR5000,00
    :50K:/DE44500105175407324931
    JOHN DOE
    BERLIN
    :59:/US12300078901234567890
    JANE SMITH
    NEW YORK
    :71A:SHA
    -}
""")

# A more complete MT103 with optional fields (FX, correspondent, regulatory).
FULL_MT103 = textwrap.dedent("""\
    {1:F01BANKFRPPXXXX1111222233}{2:I103BANKITMMXXXXN}{3:{108:USERREF999}}{4:
    :20:FULL-REF-999
    :23B:CRED
    :32A:250201USD2500,00
    :33B:EUR2300,00
    :36:1,087
    :50K:/FR7630006000011234567890189
    CLAIRE DUPONT
    PARIS
    :53B:/4000123400
    :57A:BANKITMMXXX
    :59:/IT60X0542811101000000123456
    ALFA SPA
    ROME
    :70:INVOICE INV-2025-01
    :71A:OUR
    :77B:/REG/GOODS IMPORT
    -}{5:{CHK:AABBCCDD1122}}
""")

# A message with only Block 1 and Block 4 (Blocks 2,3,5 absent – edge case).
SPARSE_MT103 = textwrap.dedent("""\
    {1:F01BANKCHZZXXXX0000000001}{4:
    :20:SPARSE-001
    :23B:CRED
    :32A:250101CHF100,00
    :50K:PAYER NAME
    :59:PAYEE NAME
    :71A:BEN
    -}
""")

# Batch file content: two messages concatenated (the standard FIN format).
BATCH_TWO = MINIMAL_MT103.strip() + "\n" + FULL_MT103.strip()


# ---------------------------------------------------------------------------
# describe_* helpers
# ---------------------------------------------------------------------------

class TestDescribeHelpers:
    def test_known_field_returns_label(self):
        assert describe_mt103_field("20") == "Transaction Reference Number"

    def test_known_field_32A(self):
        assert "Value Date" in describe_mt103_field("32A")

    def test_unknown_field_returns_fallback(self):
        result = describe_mt103_field("99Z")
        assert "99Z" in result  # the tag should appear in the fallback

    def test_known_block(self):
        desc = describe_block("4")
        assert "Text Block" in desc or "text" in desc.lower()

    def test_unknown_block(self):
        assert "9" in describe_block("9")

    def test_all_blocks_have_descriptions(self):
        for bid in ("1", "2", "3", "4", "5"):
            assert describe_block(bid) != ""

    def test_field_descriptions_nonempty(self):
        for tag, desc in MT103_FIELD_DESCRIPTIONS.items():
            assert desc, f"Description for tag {tag} is empty"


# ---------------------------------------------------------------------------
# parse_swift_mt_to_dict
# ---------------------------------------------------------------------------

class TestParseSwiftMtToDict:

    def test_blocks_extracted(self):
        result = parse_swift_mt_to_dict(MINIMAL_MT103)
        assert result["blocks"]["1"] is not None
        assert result["blocks"]["2"] is not None

    def test_block3_present_in_full_message(self):
        result = parse_swift_mt_to_dict(FULL_MT103)
        assert result["blocks"]["3"] is not None

    def test_block3_none_in_minimal_message(self):
        result = parse_swift_mt_to_dict(MINIMAL_MT103)
        assert result["blocks"]["3"] is None

    def test_block5_present_in_full_message(self):
        result = parse_swift_mt_to_dict(FULL_MT103)
        assert result["blocks"]["5"] is not None

    def test_mandatory_fields_extracted(self):
        result = parse_swift_mt_to_dict(MINIMAL_MT103)
        fields = result["text_block"]["fields"]
        assert fields["20"] == "TEST-REF-001"
        assert fields["23B"] == "CRED"
        assert fields["32A"] == "250115EUR5000,00"
        assert fields["71A"] == "SHA"

    def test_ordering_customer_multiline(self):
        """Ordering customer (:50K:) spans multiple lines – must all be captured."""
        result = parse_swift_mt_to_dict(MINIMAL_MT103)
        value = result["text_block"]["fields"]["50K"]
        assert "JOHN DOE" in value
        assert "DE44500105175407324931" in value

    def test_optional_fields_in_full_message(self):
        result = parse_swift_mt_to_dict(FULL_MT103)
        fields = result["text_block"]["fields"]
        assert fields["33B"] == "EUR2300,00"
        assert fields["36"]  == "1,087"
        assert fields["53B"] == "/4000123400"
        assert fields["57A"] == "BANKITMMXXX"
        assert "GOODS IMPORT" in fields["77B"]
        assert "INV-2025-01" in fields["70"]

    def test_sparse_message_no_block3(self):
        result = parse_swift_mt_to_dict(SPARSE_MT103)
        assert result["blocks"]["3"] is None

    def test_returns_dict_keys(self):
        result = parse_swift_mt_to_dict(MINIMAL_MT103)
        assert "blocks" in result
        assert "text_block" in result
        assert "fields" in result["text_block"]


# ---------------------------------------------------------------------------
# enrich_with_descriptions
# ---------------------------------------------------------------------------

class TestEnrichWithDescriptions:

    def test_fields_with_meta_added(self):
        parsed = parse_swift_mt_to_dict(MINIMAL_MT103)
        enriched = enrich_with_descriptions(parsed)
        assert "fields_with_meta" in enriched["text_block"]

    def test_each_field_has_description_and_value(self):
        parsed = parse_swift_mt_to_dict(MINIMAL_MT103)
        enriched = enrich_with_descriptions(parsed)
        for tag, meta in enriched["text_block"]["fields_with_meta"].items():
            assert "description" in meta, f"Tag {tag} missing description"
            assert "value" in meta,       f"Tag {tag} missing value"

    def test_known_tag_gets_correct_description(self):
        parsed = parse_swift_mt_to_dict(MINIMAL_MT103)
        enriched = enrich_with_descriptions(parsed)
        meta_20 = enriched["text_block"]["fields_with_meta"]["20"]
        assert meta_20["description"] == "Transaction Reference Number"
        assert meta_20["value"] == "TEST-REF-001"

    def test_blocks_with_meta_added(self):
        parsed = parse_swift_mt_to_dict(MINIMAL_MT103)
        enriched = enrich_with_descriptions(parsed)
        assert "blocks_with_meta" in enriched

    def test_blocks_with_meta_contains_description(self):
        parsed = parse_swift_mt_to_dict(FULL_MT103)
        enriched = enrich_with_descriptions(parsed)
        # Block 4 must have a description
        assert "description" in enriched["blocks_with_meta"]["4_raw"]


# ---------------------------------------------------------------------------
# to_mt103_message  (typed dataclass)
# ---------------------------------------------------------------------------

class TestToMt103Message:

    def _make_typed(self, raw: str) -> MT103Message:
        parsed   = parse_swift_mt_to_dict(raw)
        enriched = enrich_with_descriptions(parsed)
        return to_mt103_message(enriched)

    def test_ref_field(self):
        typed = self._make_typed(MINIMAL_MT103)
        assert typed.ref == "TEST-REF-001"

    def test_bank_op_code(self):
        typed = self._make_typed(MINIMAL_MT103)
        assert typed.bank_op_code == "CRED"

    def test_charges_sha(self):
        typed = self._make_typed(MINIMAL_MT103)
        assert typed.charges == "SHA"

    def test_charges_our(self):
        typed = self._make_typed(FULL_MT103)
        assert typed.charges == "OUR"

    def test_exchange_rate_present(self):
        typed = self._make_typed(FULL_MT103)
        assert typed.exchange_rate == "1,087"

    def test_ordering_customer_fallback(self):
        """Parser should fall back to 50A/50F if 50K is absent."""
        raw = SPARSE_MT103  # uses bare :50K: (no account prefix)
        typed = self._make_typed(raw)
        assert typed.ordering_customer is not None

    def test_summary_contains_ref(self):
        typed = self._make_typed(MINIMAL_MT103)
        assert "TEST-REF-001" in typed.summary()

    def test_summary_contains_amount(self):
        typed = self._make_typed(MINIMAL_MT103)
        assert "5000" in typed.summary()

    def test_dataclass_is_mt103message(self):
        typed = self._make_typed(MINIMAL_MT103)
        assert isinstance(typed, MT103Message)


# ---------------------------------------------------------------------------
# read_mt103_batch
# ---------------------------------------------------------------------------

class TestReadMt103Batch:

    def test_batch_splits_two_messages(self, tmp_path):
        fin_file = tmp_path / "test_batch.fin"
        fin_file.write_text(BATCH_TWO, encoding="utf-8")
        messages = read_mt103_batch(str(fin_file))
        assert len(messages) == 2

    def test_single_message_returns_list_of_one(self, tmp_path):
        fin_file = tmp_path / "single.fin"
        fin_file.write_text(MINIMAL_MT103, encoding="utf-8")
        messages = read_mt103_batch(str(fin_file))
        assert len(messages) == 1

    def test_each_message_starts_with_block1(self, tmp_path):
        fin_file = tmp_path / "batch.fin"
        fin_file.write_text(BATCH_TWO, encoding="utf-8")
        for msg in read_mt103_batch(str(fin_file)):
            assert "{1:" in msg

    def test_reads_real_sample_file(self):
        """Smoke test against the bundled mt103_transactions.fin."""
        sample = Path(__file__).resolve().parent.parent / "input" / "mt103_transactions.fin"
        if not sample.exists():
            pytest.skip("mt103_transactions.fin not found")
        messages = read_mt103_batch(str(sample))
        assert len(messages) >= 1


# ---------------------------------------------------------------------------
# write_mt103_wide_csv
# ---------------------------------------------------------------------------

class TestWriteMt103WideCsv:

    def _parse_batch(self, raw: str) -> list:
        messages = [raw]
        result = []
        for msg in messages:
            parsed = parse_swift_mt_to_dict(msg)
            result.append(enrich_with_descriptions(parsed))
        return result

    def test_csv_created(self, tmp_path):
        out = tmp_path / "out.csv"
        parsed = self._parse_batch(MINIMAL_MT103)
        write_mt103_wide_csv(parsed, str(out))
        assert out.exists()

    def test_csv_has_one_data_row(self, tmp_path):
        out = tmp_path / "out.csv"
        parsed = self._parse_batch(MINIMAL_MT103)
        write_mt103_wide_csv(parsed, str(out))
        rows = list(csv.DictReader(out.open(encoding="utf-8")))
        assert len(rows) == 1

    def test_csv_message_id_column(self, tmp_path):
        out = tmp_path / "out.csv"
        parsed = self._parse_batch(MINIMAL_MT103)
        write_mt103_wide_csv(parsed, str(out))
        rows = list(csv.DictReader(out.open(encoding="utf-8")))
        assert rows[0]["message_id"] == "1"

    def test_csv_mt_type_column(self, tmp_path):
        out = tmp_path / "out.csv"
        parsed = self._parse_batch(MINIMAL_MT103)
        write_mt103_wide_csv(parsed, str(out))
        rows = list(csv.DictReader(out.open(encoding="utf-8")))
        assert rows[0]["mt_type"] == "103"

    def test_csv_contains_field_column_for_ref(self, tmp_path):
        """The :20: field should produce a column like f_20_Transaction_Reference_Number."""
        out = tmp_path / "out.csv"
        parsed = self._parse_batch(MINIMAL_MT103)
        write_mt103_wide_csv(parsed, str(out))
        rows = list(csv.DictReader(out.open(encoding="utf-8")))
        row = rows[0]
        # Find the :20: column
        col = next((k for k in row if k.startswith("f_20_")), None)
        assert col is not None, "Expected a column starting with f_20_"
        assert row[col] == "TEST-REF-001"

    def test_csv_two_messages(self, tmp_path):
        """Batch of two messages must produce two rows."""
        out = tmp_path / "out.csv"
        parsed = []
        for raw in [MINIMAL_MT103, FULL_MT103]:
            p = parse_swift_mt_to_dict(raw)
            parsed.append(enrich_with_descriptions(p))
        write_mt103_wide_csv(parsed, str(out))
        rows = list(csv.DictReader(out.open(encoding="utf-8")))
        assert len(rows) == 2

    def test_csv_optional_field_column_exchange_rate(self, tmp_path):
        """The :36: (exchange rate) column is only present when the field exists."""
        out = tmp_path / "out.csv"
        parsed = [enrich_with_descriptions(parse_swift_mt_to_dict(FULL_MT103))]
        write_mt103_wide_csv(parsed, str(out))
        rows = list(csv.DictReader(out.open(encoding="utf-8")))
        col = next((k for k in rows[0] if "36" in k), None)
        assert col is not None
        assert rows[0][col] == "1,087"


# ---------------------------------------------------------------------------
# mt103_to_wide_row  (internal helper)
# ---------------------------------------------------------------------------

class TestMt103ToWideRow:

    def test_returns_dict(self):
        parsed = enrich_with_descriptions(parse_swift_mt_to_dict(MINIMAL_MT103))
        row = mt103_to_wide_row(parsed, message_id=42)
        assert isinstance(row, dict)

    def test_message_id_set(self):
        parsed = enrich_with_descriptions(parse_swift_mt_to_dict(MINIMAL_MT103))
        row = mt103_to_wide_row(parsed, message_id=7)
        assert row["message_id"] == 7

    def test_field_columns_prefixed_with_f(self):
        parsed = enrich_with_descriptions(parse_swift_mt_to_dict(MINIMAL_MT103))
        row = mt103_to_wide_row(parsed, message_id=1)
        field_cols = [k for k in row if k.startswith("f_")]
        assert len(field_cols) > 0
