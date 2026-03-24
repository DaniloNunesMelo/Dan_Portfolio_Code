"""
tests/test_mx_pacs008.py
========================
Unit tests for the ISO 20022 pacs.008 parser (mx_pacs008.py).

Tests use *synthetic* XML strings so no external files are required.
Each test name and docstring explains both *what* is tested and *why*
it matters for understanding the pacs.008 format.
"""

import csv
import textwrap
from pathlib import Path

import pytest

from mx_pacs008 import (
    Pacs008Transaction,
    PACS008_NS,
    _find,
    _find_text,
    _text,
    parse_pacs008,
    write_pacs008_csv,
)


# ---------------------------------------------------------------------------
# Synthetic pacs.008 XML test data
# ---------------------------------------------------------------------------

NS = f'xmlns="{PACS008_NS}"'

# Minimal valid pacs.008 – only mandatory fields present
MINIMAL_PACS008 = textwrap.dedent(f"""\
    <?xml version="1.0" encoding="UTF-8"?>
    <Document {NS}>
      <FIToFICstmrCdtTrf>
        <GrpHdr>
          <MsgId>MSG-MINIMAL-001</MsgId>
          <CreDtTm>2025-03-01T09:00:00</CreDtTm>
          <NbOfTxs>1</NbOfTxs>
          <TtlIntrBkSttlmAmt Ccy="EUR">999.99</TtlIntrBkSttlmAmt>
          <IntrBkSttlmDt>2025-03-01</IntrBkSttlmDt>
          <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>
        </GrpHdr>
        <CdtTrfTxInf>
          <PmtId>
            <EndToEndId>E2E-MIN-001</EndToEndId>
            <TxId>TXN-MIN-001</TxId>
            <UETR>550e8400-e29b-41d4-a716-000000000001</UETR>
          </PmtId>
          <IntrBkSttlmAmt Ccy="EUR">999.99</IntrBkSttlmAmt>
          <ChrgBr>SHAR</ChrgBr>
          <DbtrAgt><FinInstnId><BICFI>BANKDEFFXXX</BICFI></FinInstnId></DbtrAgt>
          <Dbtr><Nm>Min Sender</Nm></Dbtr>
          <DbtrAcct><Id><IBAN>DE44500105175407324931</IBAN></Id></DbtrAcct>
          <CdtrAgt><FinInstnId><BICFI>BANKUS33XXX</BICFI></FinInstnId></CdtrAgt>
          <Cdtr><Nm>Min Receiver</Nm></Cdtr>
          <CdtrAcct><Id><Othr><Id>US12300078901234567890</Id></Othr></Id></CdtrAcct>
        </CdtTrfTxInf>
      </FIToFICstmrCdtTrf>
    </Document>
""")

# Full pacs.008 – all mapped fields present
FULL_PACS008 = textwrap.dedent(f"""\
    <?xml version="1.0" encoding="UTF-8"?>
    <Document {NS}>
      <FIToFICstmrCdtTrf>
        <GrpHdr>
          <MsgId>MSG-FULL-42</MsgId>
          <CreDtTm>2025-06-15T14:30:00</CreDtTm>
          <NbOfTxs>1</NbOfTxs>
          <TtlIntrBkSttlmAmt Ccy="CHF">7500.00</TtlIntrBkSttlmAmt>
          <IntrBkSttlmDt>2025-06-15</IntrBkSttlmDt>
          <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>
          <InstgAgt><FinInstnId><BICFI>BANKCHZZXXX</BICFI></FinInstnId></InstgAgt>
          <InstdAgt><FinInstnId><BICFI>BANKGB22XXX</BICFI></FinInstnId></InstdAgt>
        </GrpHdr>
        <CdtTrfTxInf>
          <PmtId>
            <InstrId>INSTR-42</InstrId>
            <EndToEndId>E2E-FULL-42</EndToEndId>
            <TxId>TXN-FULL-42</TxId>
            <UETR>550e8400-e29b-41d4-a716-000000000042</UETR>
          </PmtId>
          <IntrBkSttlmAmt Ccy="CHF">7500.00</IntrBkSttlmAmt>
          <ChrgBr>DEBT</ChrgBr>
          <DbtrAgt><FinInstnId><BICFI>BANKCHZZXXX</BICFI></FinInstnId></DbtrAgt>
          <Dbtr>
            <Nm>Mario Rossi</Nm>
            <PstlAdr>
              <StrtNm>Bahnhofstrasse 10</StrtNm>
              <TwnNm>Zurich</TwnNm>
              <Ctry>CH</Ctry>
            </PstlAdr>
          </Dbtr>
          <DbtrAcct><Id><IBAN>CH9300762011623852957</IBAN></Id></DbtrAcct>
          <CdtrAgt><FinInstnId><BICFI>BANKGB22XXX</BICFI></FinInstnId></CdtrAgt>
          <Cdtr>
            <Nm>Acme Ltd</Nm>
            <PstlAdr>
              <StrtNm>42 High Street</StrtNm>
              <TwnNm>London</TwnNm>
              <Ctry>GB</Ctry>
            </PstlAdr>
          </Cdtr>
          <CdtrAcct><Id><IBAN>GB29NWBK60161331926819</IBAN></Id></CdtrAcct>
          <Purp><Cd>SALA</Cd></Purp>
          <RmtInf><Ustrd>PAYROLL JUNE 2025</Ustrd></RmtInf>
        </CdtTrfTxInf>
      </FIToFICstmrCdtTrf>
    </Document>
""")

# Batch pacs.008 – two CdtTrfTxInf elements (uncommon but valid)
BATCH_PACS008 = textwrap.dedent(f"""\
    <?xml version="1.0" encoding="UTF-8"?>
    <Document {NS}>
      <FIToFICstmrCdtTrf>
        <GrpHdr>
          <MsgId>MSG-BATCH-001</MsgId>
          <CreDtTm>2025-01-10T08:00:00</CreDtTm>
          <NbOfTxs>2</NbOfTxs>
          <TtlIntrBkSttlmAmt Ccy="EUR">200.00</TtlIntrBkSttlmAmt>
          <IntrBkSttlmDt>2025-01-10</IntrBkSttlmDt>
          <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>
        </GrpHdr>
        <CdtTrfTxInf>
          <PmtId><TxId>BATCH-TX-001</TxId><UETR>aaaabbbb-0000-0000-0000-000000000001</UETR></PmtId>
          <IntrBkSttlmAmt Ccy="EUR">100.00</IntrBkSttlmAmt>
          <ChrgBr>SHAR</ChrgBr>
          <DbtrAgt><FinInstnId><BICFI>BANKA</BICFI></FinInstnId></DbtrAgt>
          <Dbtr><Nm>Debtor A</Nm></Dbtr>
          <DbtrAcct><Id><IBAN>DE0000000000000000001</IBAN></Id></DbtrAcct>
          <CdtrAgt><FinInstnId><BICFI>BANKB</BICFI></FinInstnId></CdtrAgt>
          <Cdtr><Nm>Creditor B</Nm></Cdtr>
          <CdtrAcct><Id><IBAN>FR0000000000000000001</IBAN></Id></CdtrAcct>
        </CdtTrfTxInf>
        <CdtTrfTxInf>
          <PmtId><TxId>BATCH-TX-002</TxId><UETR>aaaabbbb-0000-0000-0000-000000000002</UETR></PmtId>
          <IntrBkSttlmAmt Ccy="EUR">100.00</IntrBkSttlmAmt>
          <ChrgBr>CRED</ChrgBr>
          <DbtrAgt><FinInstnId><BICFI>BANKC</BICFI></FinInstnId></DbtrAgt>
          <Dbtr><Nm>Debtor C</Nm></Dbtr>
          <DbtrAcct><Id><IBAN>IT0000000000000000001</IBAN></Id></DbtrAcct>
          <CdtrAgt><FinInstnId><BICFI>BANKD</BICFI></FinInstnId></CdtrAgt>
          <Cdtr><Nm>Creditor D</Nm></Cdtr>
          <CdtrAcct><Id><IBAN>ES0000000000000000001</IBAN></Id></CdtrAcct>
        </CdtTrfTxInf>
      </FIToFICstmrCdtTrf>
    </Document>
""")


# ---------------------------------------------------------------------------
# parse_pacs008 – basic parsing
# ---------------------------------------------------------------------------

class TestParsePacs008Basic:

    def test_returns_list(self):
        result = parse_pacs008(MINIMAL_PACS008)
        assert isinstance(result, list)

    def test_returns_one_transaction_for_minimal(self):
        result = parse_pacs008(MINIMAL_PACS008)
        assert len(result) == 1

    def test_transaction_type(self):
        result = parse_pacs008(MINIMAL_PACS008)
        assert isinstance(result[0], Pacs008Transaction)

    def test_parses_from_file(self, tmp_path):
        """parse_pacs008 should also accept a file path, not just a string."""
        xml_file = tmp_path / "test.xml"
        xml_file.write_text(MINIMAL_PACS008, encoding="utf-8")
        result = parse_pacs008(xml_file)
        assert len(result) == 1

    def test_parses_real_example_file(self):
        """Smoke test against the bundled mx_pacs008_example.xml."""
        sample = Path(__file__).resolve().parent.parent / "input" / "mx_pacs008_example.xml"
        if not sample.exists():
            pytest.skip("mx_pacs008_example.xml not found")
        result = parse_pacs008(sample)
        assert len(result) >= 1

    def test_raises_on_invalid_xml(self):
        with pytest.raises(Exception):
            parse_pacs008("<not-valid>")

    def test_raises_on_wrong_root_element(self):
        bad_xml = '<?xml version="1.0"?><Foo><Bar/></Foo>'
        with pytest.raises(ValueError, match="FIToFICstmrCdtTrf"):
            parse_pacs008(bad_xml)


# ---------------------------------------------------------------------------
# Group Header fields
# ---------------------------------------------------------------------------

class TestGroupHeader:

    def test_msg_id(self):
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        assert tx.msg_id == "MSG-MINIMAL-001"

    def test_created_at(self):
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        assert tx.created_at == "2025-03-01T09:00:00"

    def test_settlement_date(self):
        """IntrBkSttlmDt maps to :32A: date in MT103."""
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        assert tx.settlement_date == "2025-03-01"

    def test_total_amount(self):
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        assert tx.total_amount == "999.99"

    def test_total_currency(self):
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        assert tx.total_currency == "EUR"

    def test_inst_agent_bic(self):
        """InstgAgt (sending bank) maps to Block 1 BIC in MT103."""
        tx = parse_pacs008(FULL_PACS008)[0]
        assert tx.inst_agent_bic == "BANKCHZZXXX"

    def test_instr_agent_bic(self):
        """InstdAgt (receiving bank) maps to Block 2 BIC in MT103."""
        tx = parse_pacs008(FULL_PACS008)[0]
        assert tx.instr_agent_bic == "BANKGB22XXX"

    def test_missing_agents_are_none(self):
        """Minimal message has no InstgAgt/InstdAgt – should be None."""
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        assert tx.inst_agent_bic is None
        assert tx.instr_agent_bic is None


# ---------------------------------------------------------------------------
# Transaction-level fields  (← core of :20:, :32A:, :50K:, :59:, etc.)
# ---------------------------------------------------------------------------

class TestTransactionFields:

    def test_tx_id(self):
        """TxId is the MT103 :20: equivalent."""
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        assert tx.tx_id == "TXN-MIN-001"

    def test_uetr_present(self):
        """UETR is a new field – no MT103 equivalent. Key differentiator of ISO 20022."""
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        assert tx.uetr == "550e8400-e29b-41d4-a716-000000000001"

    def test_end_to_end_id(self):
        tx = parse_pacs008(FULL_PACS008)[0]
        assert tx.end_to_end_id == "E2E-FULL-42"

    def test_amount(self):
        """IntrBkSttlmAmt is the MT103 :32A: amount."""
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        assert tx.amount == "999.99"

    def test_currency(self):
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        assert tx.currency == "EUR"

    def test_amount_chf(self):
        tx = parse_pacs008(FULL_PACS008)[0]
        assert tx.amount == "7500.00"
        assert tx.currency == "CHF"

    def test_charge_bearer_sha(self):
        """ChrgBr=SHAR is the MT103 :71A:SHA equivalent."""
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        assert tx.charge_bearer == "SHAR"

    def test_charge_bearer_our(self):
        """ChrgBr=DEBT is the MT103 :71A:OUR equivalent."""
        tx = parse_pacs008(FULL_PACS008)[0]
        assert tx.charge_bearer == "DEBT"


# ---------------------------------------------------------------------------
# Parties (debtor / creditor)
# ---------------------------------------------------------------------------

class TestParties:

    def test_debtor_name(self):
        """Dbtr/Nm is the MT103 :50K: name line."""
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        assert tx.debtor_name == "Min Sender"

    def test_debtor_iban_from_iban_element(self):
        """DbtrAcct/Id/IBAN is the MT103 :50K: account number."""
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        assert tx.debtor_iban == "DE44500105175407324931"

    def test_debtor_iban_from_othr_element(self):
        """Some accounts use Othr/Id instead of IBAN (e.g. US accounts)."""
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        # The creditor account in MINIMAL uses Othr/Id
        assert tx.creditor_iban == "US12300078901234567890"

    def test_debtor_bic(self):
        """DbtrAgt/FinInstnId/BICFI is the MT103 :53B: correspondent."""
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        assert tx.debtor_bic == "BANKDEFFXXX"

    def test_creditor_name(self):
        """Cdtr/Nm is the MT103 :59: name line."""
        tx = parse_pacs008(FULL_PACS008)[0]
        assert tx.creditor_name == "Acme Ltd"

    def test_creditor_iban(self):
        tx = parse_pacs008(FULL_PACS008)[0]
        assert tx.creditor_iban == "GB29NWBK60161331926819"

    def test_creditor_bic(self):
        """CdtrAgt/FinInstnId/BICFI is the MT103 :57A: account-with-institution."""
        tx = parse_pacs008(FULL_PACS008)[0]
        assert tx.creditor_bic == "BANKGB22XXX"


# ---------------------------------------------------------------------------
# Payment details
# ---------------------------------------------------------------------------

class TestPaymentDetails:

    def test_remittance_info(self):
        """RmtInf/Ustrd is the MT103 :70: remittance information."""
        tx = parse_pacs008(FULL_PACS008)[0]
        assert tx.remittance_info == "PAYROLL JUNE 2025"

    def test_remittance_info_none_when_absent(self):
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        assert tx.remittance_info is None

    def test_purpose_code(self):
        """Purp/Cd is the MT103 :77B: regulatory/purpose code."""
        tx = parse_pacs008(FULL_PACS008)[0]
        assert tx.purpose_code == "SALA"

    def test_purpose_code_none_when_absent(self):
        tx = parse_pacs008(MINIMAL_PACS008)[0]
        assert tx.purpose_code is None


# ---------------------------------------------------------------------------
# Batch messages
# ---------------------------------------------------------------------------

class TestBatchMessages:

    def test_two_transactions_returned(self):
        result = parse_pacs008(BATCH_PACS008)
        assert len(result) == 2

    def test_first_transaction_tx_id(self):
        result = parse_pacs008(BATCH_PACS008)
        assert result[0].tx_id == "BATCH-TX-001"

    def test_second_transaction_tx_id(self):
        result = parse_pacs008(BATCH_PACS008)
        assert result[1].tx_id == "BATCH-TX-002"

    def test_both_share_msg_id(self):
        """Both transactions in a batch share the same group header MsgId."""
        result = parse_pacs008(BATCH_PACS008)
        assert result[0].msg_id == result[1].msg_id == "MSG-BATCH-001"

    def test_different_charge_bearers(self):
        result = parse_pacs008(BATCH_PACS008)
        assert result[0].charge_bearer == "SHAR"
        assert result[1].charge_bearer == "CRED"


# ---------------------------------------------------------------------------
# Pacs008Transaction dataclass – summary & comparison output
# ---------------------------------------------------------------------------

class TestPacs008TransactionDataclass:

    def test_summary_contains_tx_id(self):
        tx = parse_pacs008(FULL_PACS008)[0]
        assert "TXN-FULL-42" in tx.summary()

    def test_summary_contains_amount(self):
        tx = parse_pacs008(FULL_PACS008)[0]
        assert "7500.00" in tx.summary()

    def test_summary_contains_debtor(self):
        tx = parse_pacs008(FULL_PACS008)[0]
        assert "Mario Rossi" in tx.summary()

    def test_summary_contains_creditor(self):
        tx = parse_pacs008(FULL_PACS008)[0]
        assert "Acme Ltd" in tx.summary()

    def test_summary_contains_uetr(self):
        tx = parse_pacs008(FULL_PACS008)[0]
        assert tx.uetr in tx.summary()

    def test_mt103_comparison_mentions_32A(self):
        tx = parse_pacs008(FULL_PACS008)[0]
        mapping = tx.mt103_comparison()
        assert ":32A:" in mapping

    def test_mt103_comparison_mentions_uetr(self):
        """The UETR section should appear in the comparison (it has no MT103 equiv.)."""
        tx = parse_pacs008(FULL_PACS008)[0]
        mapping = tx.mt103_comparison()
        assert "UETR" in mapping

    def test_mt103_comparison_shows_correct_bic(self):
        tx = parse_pacs008(FULL_PACS008)[0]
        assert "BANKCHZZXXX" in tx.mt103_comparison()


# ---------------------------------------------------------------------------
# write_pacs008_csv
# ---------------------------------------------------------------------------

class TestWritePacs008Csv:

    def test_csv_created(self, tmp_path):
        out = tmp_path / "out.csv"
        txs = parse_pacs008(MINIMAL_PACS008)
        write_pacs008_csv(txs, str(out))
        assert out.exists()

    def test_csv_has_one_row(self, tmp_path):
        out = tmp_path / "out.csv"
        txs = parse_pacs008(MINIMAL_PACS008)
        write_pacs008_csv(txs, str(out))
        rows = list(csv.DictReader(out.open(encoding="utf-8")))
        assert len(rows) == 1

    def test_csv_msg_id_column(self, tmp_path):
        out = tmp_path / "out.csv"
        txs = parse_pacs008(MINIMAL_PACS008)
        write_pacs008_csv(txs, str(out))
        rows = list(csv.DictReader(out.open(encoding="utf-8")))
        assert rows[0]["msg_id"] == "MSG-MINIMAL-001"

    def test_csv_uetr_column(self, tmp_path):
        out = tmp_path / "out.csv"
        txs = parse_pacs008(MINIMAL_PACS008)
        write_pacs008_csv(txs, str(out))
        rows = list(csv.DictReader(out.open(encoding="utf-8")))
        assert rows[0]["uetr"] == "550e8400-e29b-41d4-a716-000000000001"

    def test_csv_two_rows_for_batch(self, tmp_path):
        out = tmp_path / "out.csv"
        txs = parse_pacs008(BATCH_PACS008)
        write_pacs008_csv(txs, str(out))
        rows = list(csv.DictReader(out.open(encoding="utf-8")))
        assert len(rows) == 2

    def test_csv_charge_bearer_column(self, tmp_path):
        out = tmp_path / "out.csv"
        txs = parse_pacs008(FULL_PACS008)
        write_pacs008_csv(txs, str(out))
        rows = list(csv.DictReader(out.open(encoding="utf-8")))
        assert rows[0]["charge_bearer"] == "DEBT"

    def test_csv_remittance_column(self, tmp_path):
        out = tmp_path / "out.csv"
        txs = parse_pacs008(FULL_PACS008)
        write_pacs008_csv(txs, str(out))
        rows = list(csv.DictReader(out.open(encoding="utf-8")))
        assert rows[0]["remittance_info"] == "PAYROLL JUNE 2025"
