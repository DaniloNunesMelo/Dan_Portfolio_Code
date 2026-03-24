"""
mt103.py
========
Educational parser for SWIFT MT103 – Single Customer Credit Transfer.

WHAT IS SWIFT MT103?
--------------------
MT103 is a SWIFT message type used to instruct an international wire transfer
of funds from one customer's account to another.  The "MT" prefix means
"Message Type" (the legacy SWIFT FIN format, plain text).

STRUCTURE OF A SWIFT MT MESSAGE
--------------------------------
Every SWIFT MT message is divided into numbered "blocks":

    {1:F01BANKDEFFXXXX1234567890}   ← Block 1: Basic Header
    {2:I103BANKUS33XXXXN}           ← Block 2: Application Header
    {3:{108:REF123456}}             ← Block 3: User Header (optional)
    {4:                             ← Block 4: TEXT BLOCK (business data)
    :20:20250101-ABC123             ← field tag : value
    :23B:CRED
    :32A:250101EUR1234,56
    ...
    -}                              ← Block 4 always ends with '-}'
    {5:{CHK:AABBCC112233}}          ← Block 5: Trailer (checksums)

Inside Block 4, each field starts with a colon-delimited tag:
    :20:   = Transaction Reference Number
    :32A:  = Value Date / Currency / Amount
    :50K:  = Ordering Customer (the sender / debtor)
    :59:   = Beneficiary Customer (the receiver / creditor)
    etc.

This module provides:
  - MT103_FIELD_DESCRIPTIONS  – human-readable tag labels
  - MT103Message dataclass     – typed representation of a parsed message
  - parse_swift_mt_to_dict()   – raw parser → nested dict
  - enrich_with_descriptions() – adds human-readable meta
  - mt103_to_wide_row()        – flattens to one CSV row per message
  - write_mt103_wide_csv()     – writes a batch to CSV
  - read_mt103_batch()         – splits a .fin file into individual messages

⚠️  Educational/demo code.  Not a certified SWIFT parser.
    Do not use for production payment processing without proper validation.
"""

import csv
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Field & block metadata
# ---------------------------------------------------------------------------

# Maps every MT103 tag code → human-readable description.
# Tags with letter suffixes (50K, 57A …) are format variants of the same field.
MT103_FIELD_DESCRIPTIONS: Dict[str, str] = {
    # ── Mandatory fields ─────────────────────────────────────────────────
    "20":  "Transaction Reference Number",     # unique ref assigned by sender
    "23B": "Bank Operation Code",              # always CRED for MT103
    "32A": "Value Date / Currency / Interbank Settled Amount",  # e.g. 250101EUR1234,56
    "50A": "Ordering Customer",                # sender's BIC + account
    "50F": "Ordering Customer",                # sender's party ID style
    "50K": "Ordering Customer",                # sender's free-text style (most common)
    "59":  "Beneficiary Customer",             # receiver – no account prefix
    "59A": "Beneficiary Customer",             # receiver – BIC style
    "71A": "Details of Charges",               # SHA / OUR / BEN
    # ── Optional fields ──────────────────────────────────────────────────
    "13C": "Time Indication",                  # local time + UTC offset
    "23E": "Instruction Code",                 # e.g. SDVA (same-day value)
    "26T": "Transaction Type Code",            # business classification
    "33B": "Currency / Original Ordered Amount",  # amount before FX conversion
    "36":  "Exchange Rate",                    # FX rate applied
    "51A": "Sending Institution",              # BIC of the sending bank
    "52A": "Ordering Institution",             # BIC of sender's bank
    "52D": "Ordering Institution",             # free-text variant
    "53A": "Sender's Correspondent",           # nostro/vostro intermediary BIC
    "53B": "Sender's Correspondent",           # account variant
    "53D": "Sender's Correspondent",           # free-text variant
    "54A": "Receiver's Correspondent",         # receiver-side intermediary
    "55A": "Third Reimbursement Institution",
    "56A": "Intermediary Institution",         # for complex routing
    "57A": "Account With Institution (Beneficiary's Bank)",  # BIC
    "57B": "Account With Institution (Beneficiary's Bank)",  # location
    "57C": "Account With Institution (Beneficiary's Bank)",  # account only
    "57D": "Account With Institution (Beneficiary's Bank)",  # free text
    "70":  "Remittance Information",           # payment reference / invoice #
    "71F": "Sender's Charges",                 # itemised charges (OUR)
    "71G": "Receiver's Charges",               # itemised charges (BEN)
    "72":  "Sender to Receiver Information",   # bank-to-bank instructions
    "77B": "Regulatory Reporting",             # AML / compliance codes
    "77T": "Envelope Contents",                # e-envelope extension
}

# Maps block ID → description for human-readable output
BLOCK_DESCRIPTIONS: Dict[str, str] = {
    "1": "Basic Header Block       – sender BIC, session & sequence",
    "2": "Application Header Block – message type (103), receiver BIC",
    "3": "User Header Block        – optional message reference overrides",
    "4": "Text Block               – all business payment fields (:20:, :32A: …)",
    "5": "Trailer Block            – checksums (CHK) and possible PDE flags",
}


# ---------------------------------------------------------------------------
# Typed representation
# ---------------------------------------------------------------------------

@dataclass
class MT103Message:
    """
    A parsed MT103 payment message.

    Fields mirror the most common MT103 tags.  Any tag absent in the raw
    message will be None.
    """
    # Block headers (raw text extracted from {1:…} etc.)
    block_1: Optional[str] = None  # Basic header
    block_2: Optional[str] = None  # Application header
    block_3: Optional[str] = None  # User header
    block_5: Optional[str] = None  # Trailer

    # Core business fields
    ref:            Optional[str] = None  # :20:  Transaction Reference
    bank_op_code:   Optional[str] = None  # :23B: always "CRED"
    value_date_ccy_amount: Optional[str] = None  # :32A: e.g. "250101EUR1234,56"
    orig_amount:    Optional[str] = None  # :33B: amount before FX
    exchange_rate:  Optional[str] = None  # :36:

    # Parties
    ordering_customer:    Optional[str] = None  # :50K/50A/50F – sender
    beneficiary_customer: Optional[str] = None  # :59/59A – receiver

    # Banks / routing
    senders_correspondent:   Optional[str] = None  # :53A/53B/53D
    account_with_institution: Optional[str] = None  # :57A/57B/57C/57D

    # Payment details
    remittance_info: Optional[str] = None  # :70:
    charges:         Optional[str] = None  # :71A: SHA | OUR | BEN
    regulatory:      Optional[str] = None  # :77B:

    # All raw fields (tag → value) for any tag not listed above
    raw_fields: Dict[str, str] = field(default_factory=dict)

    def summary(self) -> str:
        """One-line human-readable summary of this payment."""
        return (
            f"[{self.ref}] "
            f"{self.value_date_ccy_amount or '?'} | "
            f"Sender: {(self.ordering_customer or '?').splitlines()[0]} → "
            f"Benef: {(self.beneficiary_customer or '?').splitlines()[0]} | "
            f"Charges: {self.charges or '?'}"
        )


# ---------------------------------------------------------------------------
# Helper look-ups
# ---------------------------------------------------------------------------

def describe_mt103_field(tag: str) -> str:
    """Return the human-readable label for a given MT103 tag, or a fallback."""
    return MT103_FIELD_DESCRIPTIONS.get(tag, f"Unknown MT103 field {tag}")


def describe_block(block_id: str) -> str:
    """Return the human-readable description for a SWIFT block number."""
    return BLOCK_DESCRIPTIONS.get(block_id, f"Unknown block {block_id}")


# ---------------------------------------------------------------------------
# Core parser
# ---------------------------------------------------------------------------

def parse_swift_mt_to_dict(message: str) -> Dict[str, Any]:
    """
    Parse a raw SWIFT MT message string into a nested dictionary.

    Algorithm
    ---------
    1. Use a regex to find every ``{N:…}`` block.
    2. Extract Block 4 (the text block) and split it into individual fields
       by scanning for ``:XX:`` tag markers.
    3. Return a nested dict with ``blocks`` (raw) and ``text_block.fields``.

    Parameters
    ----------
    message : str
        Raw MT message text, e.g. the output of ``read_mt103_batch()``.

    Returns
    -------
    dict with keys:
      - ``blocks``     : raw content of blocks 1–5
      - ``text_block`` : ``{ raw, fields }``
    """
    # Step 1 – split the message into numbered blocks ─────────────────────
    # Regex matches:  {1:content}  {2:content}  {4:\n...\n-}  etc.
    block_pattern = re.compile(r"\{(\d):([^}]*)\}", re.DOTALL)
    blocks: Dict[str, str] = {}

    for block_id, content in block_pattern.findall(message):
        blocks[block_id] = content

    # Step 2 – clean up the text block (Block 4) ──────────────────────────
    text_block = (blocks.get("4") or "").strip()
    if text_block.endswith("-"):
        text_block = text_block[:-1].rstrip()

    # Step 3 – split Block 4 into individual fields ───────────────────────
    # Each field starts with :XX: or :XXX:  (2-digit tag + optional letter)
    field_pattern = re.compile(r"\s*:(\d{2}[A-Z]?):")
    fields: Dict[str, str] = {}

    matches = list(field_pattern.finditer(text_block))
    for i, m in enumerate(matches):
        tag = m.group(1)
        start = m.end()
        # Value runs until the next tag marker (or end of block)
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text_block)
        value = text_block[start:end].strip()
        fields[tag] = value

    return {
        "blocks": {
            "1": blocks.get("1"),
            "2": blocks.get("2"),
            "3": blocks.get("3"),
            "4_raw": blocks.get("4"),
            "5": blocks.get("5"),
        },
        "text_block": {
            "raw": text_block,
            "fields": fields,
        },
    }


def enrich_with_descriptions(parsed: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add human-readable ``description`` to every field in ``text_block.fields``.

    Mutates and returns the same dict (adds ``fields_with_meta`` and
    ``blocks_with_meta`` keys).
    """
    fields = parsed["text_block"]["fields"]

    # Each tag gets a description from MT103_FIELD_DESCRIPTIONS
    parsed["text_block"]["fields_with_meta"] = {
        tag: {
            "description": describe_mt103_field(tag),
            "value": value,
        }
        for tag, value in fields.items()
    }

    # Blocks also get descriptions
    parsed["blocks_with_meta"] = {
        b_id: {
            "description": describe_block(b_id),
            "value": b_value,
        }
        for b_id, b_value in parsed["blocks"].items()
        if b_value is not None
    }

    return parsed


def to_mt103_message(parsed: Dict[str, Any]) -> MT103Message:
    """
    Convert a parsed + enriched dict into a typed ``MT103Message`` dataclass.

    Use this when you want IDE auto-complete and dot-access to fields
    instead of dict key lookups.
    """
    f = parsed["text_block"]["fields"]
    b = parsed["blocks"]

    return MT103Message(
        block_1=b.get("1"),
        block_2=b.get("2"),
        block_3=b.get("3"),
        block_5=b.get("5"),
        ref=f.get("20"),
        bank_op_code=f.get("23B"),
        value_date_ccy_amount=f.get("32A"),
        orig_amount=f.get("33B"),
        exchange_rate=f.get("36"),
        # :50K: is the most common ordering-customer variant; fall back to 50A/50F
        ordering_customer=f.get("50K") or f.get("50A") or f.get("50F"),
        # :59: (no suffix) is most common; :59A: is BIC-based
        beneficiary_customer=f.get("59") or f.get("59A"),
        senders_correspondent=f.get("53B") or f.get("53A") or f.get("53D"),
        account_with_institution=(
            f.get("57A") or f.get("57B") or f.get("57C") or f.get("57D")
        ),
        remittance_info=f.get("70"),
        charges=f.get("71A"),
        regulatory=f.get("77B"),
        raw_fields=f,
    )


# ---------------------------------------------------------------------------
# CSV export helpers
# ---------------------------------------------------------------------------

def _slugify_description(desc: str) -> str:
    """Convert a tag description to a safe CSV column name.

    Example: "Value Date / Currency / Interbank Settled Amount"
             → "Value_Date_Currency_Interbank_Settled_Amount"
    """
    return re.sub(r"[^0-9a-zA-Z]+", "_", desc).strip("_")


def mt103_to_wide_row(parsed: Dict[str, Any], message_id: int, mt_type: str = "103") -> Dict[str, Any]:
    """
    Flatten one enriched MT103 dict into a single *wide* CSV row.

    Column names follow the pattern ``f_{TAG}_{Description_Slugified}``,
    e.g. ``f_32A_Value_Date_Currency_Interbank_Settled_Amount``.

    Parameters
    ----------
    parsed     : enriched dict from ``enrich_with_descriptions()``
    message_id : sequential integer, used as primary key in the CSV
    mt_type    : always "103" for MT103; kept for extensibility
    """
    blocks = parsed.get("blocks", {})
    fields_meta = parsed.get("text_block", {}).get("fields_with_meta", {})

    # Start with block-level columns
    row: Dict[str, Any] = {
        "message_id": message_id,
        "mt_type": mt_type,
        "block_1": blocks.get("1"),
        "block_2": blocks.get("2"),
        "block_3": blocks.get("3"),
        "block_4_raw": blocks.get("4_raw"),
        "block_5": blocks.get("5"),
    }

    # Add one column per SWIFT field: f_{tag}_{Description}
    for tag, meta in fields_meta.items():
        desc = meta.get("description", "")
        value = meta.get("value", "")
        col_name = f"f_{tag}_{_slugify_description(desc)}" if desc else f"f_{tag}"
        row[col_name] = value

    return row


def write_mt103_wide_csv(parsed_messages: List[Dict[str, Any]], csv_path: str) -> None:
    """
    Write a list of enriched MT103 dicts to a wide-format CSV file.

    The CSV has one row per payment and one column per SWIFT tag.
    This format is ideal for loading into a data warehouse or analytics tool.

    Parameters
    ----------
    parsed_messages : list of dicts from ``enrich_with_descriptions()``
    csv_path        : output file path (e.g. "mt103_wide_batch.csv")
    """
    rows = [mt103_to_wide_row(p, i) for i, p in enumerate(parsed_messages, start=1)]

    # Fixed columns first, then all field columns sorted alphabetically
    base_cols = ["message_id", "mt_type", "block_1", "block_2", "block_3", "block_4_raw", "block_5"]
    field_cols = sorted({k for row in rows for k in row if k.startswith("f_")})
    columns = base_cols + field_cols

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


# ---------------------------------------------------------------------------
# Batch file reader
# ---------------------------------------------------------------------------

def read_mt103_batch(path: str) -> List[str]:
    """
    Read a SWIFT FIN file and split it into a list of individual MT message strings.

    A FIN batch file simply concatenates multiple messages one after another.
    A new message always starts with ``{1:``, so we use that as the delimiter.

    Parameters
    ----------
    path : path to a ``.fin`` file containing one or more MT103 messages

    Returns
    -------
    List of raw message strings, one per MT103 transaction.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = f.read()

    raw_messages: List[str] = []
    current: List[str] = []

    for line in data.splitlines():
        # Detect the start of a new message
        if line.startswith("{1:") and current:
            raw_messages.append("\n".join(current))
            current = [line]
        else:
            current.append(line)

    if current:
        raw_messages.append("\n".join(current))

    return raw_messages


# ---------------------------------------------------------------------------
# CLI entry point (for quick testing)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    batch_file = sys.argv[1] if len(sys.argv) > 1 else "input/mt103_transactions.fin"
    csv_out = "output/mt103_wide_batch.csv"
    Path(csv_out).parent.mkdir(parents=True, exist_ok=True)

    raw_msgs = read_mt103_batch(batch_file)
    print(f"Found {len(raw_msgs)} MT103 message(s) in '{batch_file}'")

    parsed_list = []
    for i, msg in enumerate(raw_msgs, start=1):
        parsed = parse_swift_mt_to_dict(msg)
        enriched = enrich_with_descriptions(parsed)
        typed = to_mt103_message(enriched)
        print(f"  [{i}] {typed.summary()}")
        parsed_list.append(enriched)

    write_mt103_wide_csv(parsed_list, csv_out)
    print(f"\n✅  Written {len(parsed_list)} row(s) to '{csv_out}'")
