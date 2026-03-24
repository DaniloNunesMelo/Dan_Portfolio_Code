"""
mx_pacs008.py
=============
Educational parser for ISO 20022 pacs.008 – Customer Credit Transfer.

WHAT IS ISO 20022 / MX?
-----------------------
ISO 20022 is the new global standard for financial messaging.  SWIFT calls
these messages "MX" (as opposed to legacy "MT" messages).  They are XML-based,
richer, and more structured than the flat-text MT format.

The pacs.008 message is the *direct replacement* for MT103:

    MT103  →  pacs.008.001.xx  (Customer Credit Transfer)

The main benefits of MX over MT:
  ✓ Structured XML – no free-text ambiguity in names or addresses
  ✓ UETR           – a UUID4 that uniquely tracks a payment end-to-end
  ✓ Rich data       – full legal names, purpose codes, LEI codes
  ✓ Interoperable   – same format for SWIFT gpi, SEPA, TARGET2, FedNow

MT103 → pacs.008 FIELD MAPPING (quick reference)
-------------------------------------------------
MT103 tag │ pacs.008 XML element                  │ Description
──────────┼───────────────────────────────────────┼──────────────────────────
:20:      │ CdtTrfTxInf/PmtId/TxId                │ Transaction reference
(n/a)     │ CdtTrfTxInf/PmtId/UETR                │ NEW universal tracker
:32A: dt  │ GrpHdr/IntrBkSttlmDt                  │ Settlement date
:32A: amt │ CdtTrfTxInf/IntrBkSttlmAmt @Ccy       │ Amount + currency
:33B:     │ CdtTrfTxInf/InstdAmt @Ccy              │ Instructed amount
:36:      │ CdtTrfTxInf/XchgRate                  │ Exchange rate
:50K:     │ CdtTrfTxInf/Dbtr                      │ Debtor (sender)
:50K: acct│ CdtTrfTxInf/DbtrAcct/Id/IBAN          │ Sender's IBAN
:53B:     │ CdtTrfTxInf/DbtrAgt/FinInstnId/BICFI  │ Sender's bank BIC
:57A:     │ CdtTrfTxInf/CdtrAgt/FinInstnId/BICFI  │ Receiver's bank BIC
:59:      │ CdtTrfTxInf/Cdtr                      │ Creditor (receiver)
:59: acct │ CdtTrfTxInf/CdtrAcct/Id/IBAN          │ Receiver's IBAN
:70:      │ CdtTrfTxInf/RmtInf/Ustrd              │ Remittance information
:71A:     │ CdtTrfTxInf/ChrgBr                    │ Charge bearer
:77B:     │ CdtTrfTxInf/Purp/Cd                   │ Purpose / regulatory code

⚠️  Educational/demo code.  Not a certified SWIFT parser.
"""

import csv
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree as ET


# ---------------------------------------------------------------------------
# ISO 20022 namespace
# ---------------------------------------------------------------------------

# The XML namespace URI for pacs.008.001.08.
# All element names must be resolved relative to this namespace.
PACS008_NS = "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"

# Convenience prefix used in XPath calls: {namespace}ElementName
_NS = f"{{{PACS008_NS}}}"


# ---------------------------------------------------------------------------
# Typed representation
# ---------------------------------------------------------------------------

@dataclass
class Pacs008Transaction:
    """
    A single credit-transfer transaction extracted from a pacs.008 message.

    This mirrors the MT103Message dataclass in mt103.py so you can compare
    the two formats side by side.
    """
    # ── Group Header fields (apply to the whole message) ─────────────────
    msg_id:         Optional[str] = None  # GrpHdr/MsgId
    created_at:     Optional[str] = None  # GrpHdr/CreDtTm
    settlement_date: Optional[str] = None  # GrpHdr/IntrBkSttlmDt  ← :32A: date
    total_amount:   Optional[str] = None  # GrpHdr/TtlIntrBkSttlmAmt
    total_currency: Optional[str] = None  # GrpHdr/TtlIntrBkSttlmAmt @Ccy
    inst_agent_bic: Optional[str] = None  # GrpHdr/InstgAgt BIC    ← Block 1
    instr_agent_bic: Optional[str] = None # GrpHdr/InstdAgt BIC    ← Block 2

    # ── Transaction-level fields ──────────────────────────────────────────
    tx_id:          Optional[str] = None  # PmtId/TxId              ← :20:
    end_to_end_id:  Optional[str] = None  # PmtId/EndToEndId
    uetr:           Optional[str] = None  # PmtId/UETR              ← NEW (no MT equiv.)

    # Settlement amount  ← :32A: amount + currency
    amount:         Optional[str] = None  # CdtTrfTxInf/IntrBkSttlmAmt
    currency:       Optional[str] = None  # CdtTrfTxInf/IntrBkSttlmAmt @Ccy

    # Charge bearer  ← :71A:  (SHAR=SHA / DEBT=OUR / CRED=BEN)
    charge_bearer:  Optional[str] = None

    # Parties  ← :50K: and :59:
    debtor_name:    Optional[str] = None  # sender's full name
    debtor_iban:    Optional[str] = None  # sender's IBAN
    debtor_bic:     Optional[str] = None  # sender's bank BIC   ← :53B:
    creditor_name:  Optional[str] = None  # receiver's full name
    creditor_iban:  Optional[str] = None  # receiver's account  ← :59:
    creditor_bic:   Optional[str] = None  # receiver's bank BIC ← :57A:

    # Payment details  ← :70: and :77B:
    remittance_info: Optional[str] = None
    purpose_code:   Optional[str] = None

    # Any extra raw fields not mapped above
    extras: Dict[str, str] = field(default_factory=dict)

    def summary(self) -> str:
        """One-line summary, parallel to MT103Message.summary()."""
        return (
            f"[{self.tx_id}] "
            f"{self.currency} {self.amount} | "
            f"Debtor: {self.debtor_name or '?'} → "
            f"Creditor: {self.creditor_name or '?'} | "
            f"UETR: {self.uetr or 'n/a'}"
        )

    def mt103_comparison(self) -> str:
        """
        Return a human-readable side-by-side comparison with MT103 field names.
        Useful for educational demos.
        """
        lines = [
            "  MT103 field  →  MX pacs.008 value",
            "  ───────────────────────────────────────────────────────",
            f"  :20:         →  TxId          : {self.tx_id}",
            f"  (n/a)        →  UETR          : {self.uetr}",
            f"  :32A: date   →  SttlmDt       : {self.settlement_date}",
            f"  :32A: amount →  Amount/Ccy    : {self.currency} {self.amount}",
            f"  :50K:        →  Debtor        : {self.debtor_name}",
            f"  :50K: IBAN   →  DebtorAcct    : {self.debtor_iban}",
            f"  :53B:        →  DbtrAgt BIC   : {self.debtor_bic}",
            f"  :57A:        →  CdtrAgt BIC   : {self.creditor_bic}",
            f"  :59:         →  Creditor      : {self.creditor_name}",
            f"  :59: IBAN    →  CdtrAcct      : {self.creditor_iban}",
            f"  :70:         →  RmtInf        : {self.remittance_info}",
            f"  :71A:        →  ChrgBr        : {self.charge_bearer}",
            f"  :77B:        →  Purpose       : {self.purpose_code}",
        ]
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# XML helper
# ---------------------------------------------------------------------------

def _text(element: Optional[ET.Element]) -> Optional[str]:
    """Return stripped text content of an XML element, or None if absent."""
    if element is None:
        return None
    t = element.text
    return t.strip() if t else None


def _find(root: ET.Element, *path_parts: str) -> Optional[ET.Element]:
    """
    Walk a sequence of tag names from *root*, applying the ISO 20022 namespace.

    Example:
        _find(root, "GrpHdr", "MsgId")
        # equivalent to root.find(f"{_NS}GrpHdr/{_NS}MsgId")
    """
    node = root
    for part in path_parts:
        node = node.find(f"{_NS}{part}")
        if node is None:
            return None
    return node


def _find_text(root: ET.Element, *path_parts: str) -> Optional[str]:
    """Shortcut: walk path and return the text content (or None)."""
    return _text(_find(root, *path_parts))


# ---------------------------------------------------------------------------
# Core parser
# ---------------------------------------------------------------------------

def parse_pacs008(xml_source: str | Path) -> List[Pacs008Transaction]:
    """
    Parse a pacs.008.001.08 XML file (or string) and return a list of
    ``Pacs008Transaction`` objects – one per ``<CdtTrfTxInf>`` element.

    A single pacs.008 message can carry multiple transactions (batch),
    but typically carries just one (like MT103).

    Parameters
    ----------
    xml_source : path to an XML file, or a raw XML string

    Returns
    -------
    List of parsed transactions (usually length 1).
    """
    # Parse XML ──────────────────────────────────────────────────────────
    if isinstance(xml_source, Path) or (
        isinstance(xml_source, str) and not xml_source.strip().startswith("<")
    ):
        tree = ET.parse(str(xml_source))
        root = tree.getroot()
    else:
        root = ET.fromstring(xml_source)

    # Navigate to the FIToFICstmrCdtTrf element ──────────────────────────
    # The root IS <Document>, so we step into <FIToFICstmrCdtTrf>
    trf = root.find(f"{_NS}FIToFICstmrCdtTrf")
    if trf is None:
        # Try without namespace (some test messages skip it)
        trf = root.find("FIToFICstmrCdtTrf")
    if trf is None:
        raise ValueError(
            "XML does not contain a <FIToFICstmrCdtTrf> element. "
            "Is this a valid pacs.008 message?"
        )

    # Parse Group Header (shared across all transactions) ─────────────────
    grp = _find(trf, "GrpHdr")
    if grp is None:
        raise ValueError("Missing <GrpHdr> in pacs.008 message.")

    msg_id          = _find_text(grp, "MsgId")
    created_at      = _find_text(grp, "CreDtTm")
    settlement_date = _find_text(grp, "IntrBkSttlmDt")

    # Total amount (element text) + currency (attribute)
    total_el = _find(grp, "TtlIntrBkSttlmAmt")
    total_amount   = _text(total_el)
    total_currency = total_el.attrib.get("Ccy") if total_el is not None else None

    # Sending / receiving agents (= Block 1 / Block 2 in MT103)
    inst_agent_bic  = _find_text(grp, "InstgAgt", "FinInstnId", "BICFI")
    instr_agent_bic = _find_text(grp, "InstdAgt", "FinInstnId", "BICFI")

    # Parse individual transactions ───────────────────────────────────────
    transactions: List[Pacs008Transaction] = []

    for tx in trf.findall(f"{_NS}CdtTrfTxInf"):
        # Payment IDs  ← :20:
        pmt_id = _find(tx, "PmtId")
        tx_id       = _find_text(pmt_id, "TxId")         if pmt_id is not None else None
        end_to_end  = _find_text(pmt_id, "EndToEndId")   if pmt_id is not None else None
        uetr        = _find_text(pmt_id, "UETR")          if pmt_id is not None else None

        # Settlement amount + currency  ← :32A:
        amt_el   = _find(tx, "IntrBkSttlmAmt")
        amount   = _text(amt_el)
        currency = amt_el.attrib.get("Ccy") if amt_el is not None else None

        # Charge bearer  ← :71A:
        charge_bearer = _find_text(tx, "ChrgBr")

        # Debtor (sender)  ← :50K:
        debtor_name = _find_text(tx, "Dbtr", "Nm")
        debtor_iban = (
            _find_text(tx, "DbtrAcct", "Id", "IBAN")
            or _find_text(tx, "DbtrAcct", "Id", "Othr", "Id")
        )
        debtor_bic  = _find_text(tx, "DbtrAgt", "FinInstnId", "BICFI")

        # Creditor (receiver)  ← :59:
        creditor_name = _find_text(tx, "Cdtr", "Nm")
        creditor_iban = (
            _find_text(tx, "CdtrAcct", "Id", "IBAN")
            or _find_text(tx, "CdtrAcct", "Id", "Othr", "Id")
        )
        creditor_bic  = _find_text(tx, "CdtrAgt", "FinInstnId", "BICFI")

        # Remittance info  ← :70:
        remittance_info = _find_text(tx, "RmtInf", "Ustrd")

        # Purpose code  ← :77B:
        purpose_code = _find_text(tx, "Purp", "Cd")

        transactions.append(Pacs008Transaction(
            msg_id=msg_id,
            created_at=created_at,
            settlement_date=settlement_date,
            total_amount=total_amount,
            total_currency=total_currency,
            inst_agent_bic=inst_agent_bic,
            instr_agent_bic=instr_agent_bic,
            tx_id=tx_id,
            end_to_end_id=end_to_end,
            uetr=uetr,
            amount=amount,
            currency=currency,
            charge_bearer=charge_bearer,
            debtor_name=debtor_name,
            debtor_iban=debtor_iban,
            debtor_bic=debtor_bic,
            creditor_name=creditor_name,
            creditor_iban=creditor_iban,
            creditor_bic=creditor_bic,
            remittance_info=remittance_info,
            purpose_code=purpose_code,
        ))

    return transactions


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def write_pacs008_csv(transactions: List[Pacs008Transaction], csv_path: str) -> None:
    """
    Write a list of Pacs008Transaction objects to a flat CSV file.

    Columns are named to echo the pacs.008 element path, with a comment
    showing the equivalent MT103 tag.
    """
    columns = [
        "msg_id",           # GrpHdr/MsgId
        "created_at",       # GrpHdr/CreDtTm
        "settlement_date",  # GrpHdr/IntrBkSttlmDt       ← :32A: date
        "currency",         # IntrBkSttlmAmt @Ccy         ← :32A: ccy
        "amount",           # IntrBkSttlmAmt              ← :32A: amount
        "tx_id",            # PmtId/TxId                  ← :20:
        "end_to_end_id",    # PmtId/EndToEndId
        "uetr",             # PmtId/UETR                  ← NEW (no MT equiv.)
        "charge_bearer",    # ChrgBr                      ← :71A:
        "debtor_name",      # Dbtr/Nm                     ← :50K: name
        "debtor_iban",      # DbtrAcct/Id/IBAN            ← :50K: account
        "debtor_bic",       # DbtrAgt/FinInstnId/BICFI    ← :53B:
        "creditor_bic",     # CdtrAgt/FinInstnId/BICFI    ← :57A:
        "creditor_name",    # Cdtr/Nm                     ← :59: name
        "creditor_iban",    # CdtrAcct/Id/IBAN            ← :59: account
        "remittance_info",  # RmtInf/Ustrd                ← :70:
        "purpose_code",     # Purp/Cd                     ← :77B:
    ]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for tx in transactions:
            writer.writerow({col: getattr(tx, col, None) for col in columns})


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    xml_file = sys.argv[1] if len(sys.argv) > 1 else "input/mx_pacs008_example.xml"
    csv_out  = "output/mx_pacs008_output.csv"
    Path(csv_out).parent.mkdir(parents=True, exist_ok=True)

    transactions = parse_pacs008(xml_file)
    print(f"Parsed {len(transactions)} transaction(s) from '{xml_file}'\n")

    for i, tx in enumerate(transactions, start=1):
        print(f"Transaction {i}:")
        print(f"  {tx.summary()}")
        print()
        print(tx.mt103_comparison())
        print()

    write_pacs008_csv(transactions, csv_out)
    print(f"✅  Written {len(transactions)} row(s) to '{csv_out}'")
