import csv
import re
from typing import Dict, Any

MT103_FIELD_DESCRIPTIONS = {
    "20":  "Transaction Reference Number",
    "23B": "Bank Operation Code",
    "32A": "Value Date / Currency / Interbank Settled Amount",
    "33B": "Currency / Original Ordered Amount",
    "36": "Exchange Rate",
    "50A": "Ordering Customer",
    "50F": "Ordering Customer",
    "50K": "Ordering Customer",
    "53A": "Sender's Correspondent",
    "53B": "Sender's Correspondent",
    "53D": "Sender's Correspondent",
    "57A": "Account With Institution (Beneficiary's Bank)",
    "57B": "Account With Institution (Beneficiary's Bank)",
    "57C": "Account With Institution (Beneficiary's Bank)",
    "57D": "Account With Institution (Beneficiary's Bank)",
    "59":  "Beneficiary Customer",
    "59A": "Beneficiary Customer",
    "70":  "Remittance Information",
    "71A": "Details of Charges",
    "77B": "Regulatory Reporting",
    # add more as needed...
}

BLOCK_DESCRIPTIONS = {
    "1": "Basic Header Block",
    "2": "Application Header Block",
    "3": "User Header Block",
    "4": "Text Block",
    "5": "Trailer Block",
}


def describe_mt103_field(tag: str) -> str:
    return MT103_FIELD_DESCRIPTIONS.get(tag, f"Unknown MT103 field {tag}")


def describe_block(block_id: str) -> str:
    return BLOCK_DESCRIPTIONS.get(block_id, f"Unknown block {block_id}")


def parse_swift_mt_to_dict(message: str) -> Dict[str, Any]:
    """
    Parse a SWIFT MT message into a nested dict.
    """
    block_pattern = re.compile(r"\{(\d):([^}]*)\}")
    blocks: Dict[str, str] = {}

    for block_id, content in block_pattern.findall(message):
        blocks[block_id] = content

    text_block = blocks.get("4", "") or ""
    text_block = text_block.strip()

    if text_block.endswith("-"):
        text_block = text_block[:-1].rstrip()

    field_pattern = re.compile(r"\s*:(\d{2}[A-Z]?):")
    fields: Dict[str, str] = {}

    matches = list(field_pattern.finditer(text_block))
    for i, m in enumerate(matches):
        tag = m.group(1)
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text_block)
        value = text_block[start:end].strip()
        fields[tag] = value

    result: Dict[str, Any] = {
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

    return result


def enrich_with_descriptions(parsed: dict) -> dict:
    fields = parsed["text_block"]["fields"]

    parsed["text_block"]["fields_with_meta"] = {
        tag: {
            "description": describe_mt103_field(tag),
            "value": value,
        }
        for tag, value in fields.items()
    }

    parsed["blocks_with_meta"] = {
        b_id: {
            "description": describe_block(b_id),
            "value": b_value,
        }
        for b_id, b_value in parsed["blocks"].items()
        if b_value is not None
    }

    return parsed


def _slugify_description(desc: str) -> str:
    """Make a description safe for use in a CSV column name."""
    # Replace non-alphanumeric with underscores, trim extra underscores
    return re.sub(r"[^0-9a-zA-Z]+", "_", desc).strip("_")


def mt103_to_wide_row(parsed: dict, message_id: int, mt_type: str = "103") -> dict:
    """
    Flatten one parsed MT103 into a single 'wide' row.
    Column names include both tag and description.
    """
    blocks = parsed.get("blocks", {})
    fields_meta = (
        parsed.get("text_block", {})
        .get("fields_with_meta", {})
    )

    row = {
        "message_id": message_id,
        "mt_type": mt_type,
        "block_1": blocks.get("1"),
        "block_2": blocks.get("2"),
        "block_3": blocks.get("3"),
        "block_4_raw": blocks.get("4_raw"),
        "block_5": blocks.get("5"),
    }

    # Add each SWIFT field as a separate column:
    # e.g. f_20_Transaction_Reference_Number
    for tag, meta in fields_meta.items():
        desc = meta.get("description", "")
        value = meta.get("value", "")
        if desc:
            slug = _slugify_description(desc)
            col_name = f"f_{tag}_{slug}"
        else:
            col_name = f"f_{tag}"
        row[col_name] = value

    return row


def write_mt103_wide_csv(parsed_messages, csv_path: str):
    """
    parsed_messages: iterable of result_dict-like objects (already enriched)
    csv_path: where to write the CSV
    """
    rows = []

    for i, parsed in enumerate(parsed_messages, start=1):
        row = mt103_to_wide_row(parsed, message_id=i)
        rows.append(row)

    base_cols = [
        "message_id",
        "mt_type",
        "block_1",
        "block_2",
        "block_3",
        "block_4_raw",
        "block_5",
    ]
    field_cols = sorted(
        {key for row in rows for key in row.keys() if key.startswith("f_")}
    )
    columns = base_cols + field_cols

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

def read_mt103_batch(path: str):
    with open(path, "r", encoding="utf-8") as f:
        data = f.read()

    # Split whenever a new message starts: look for '{1:'
    raw_messages = []
    current = []

    for line in data.splitlines():
        line = line.rstrip("\n")
        if line.startswith("{1:") and current:
            raw_messages.append("".join(current))
            current = [line]
        else:
            current.append(line)
    if current:
        raw_messages.append("".join(current))

    return raw_messages


if __name__ == "__main__":
    batch_file = "mt103_transactions.fin"
    raw_msgs = read_mt103_batch(batch_file)

    parsed_list = []
    for msg in raw_msgs:
        parsed = parse_swift_mt_to_dict(msg)
        parsed_desc = enrich_with_descriptions(parsed)
        parsed_list.append(parsed_desc)

    total_trans = len(parsed_list)
    print("Total Transactions: ", total_trans)
    write_mt103_wide_csv(parsed_list, "mt103_wide_batch.csv")
    print("Written mt103_wide_batch.csv")
