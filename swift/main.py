"""
main.py
=======
Didactic entry point for the SWIFT payment parsing demo.

Runs through the complete workflow step by step:

  Step 1 – Read & parse a batch of MT103 messages  (legacy FIN format)
  Step 2 – Enrich fields with human-readable descriptions
  Step 3 – Print a per-payment summary
  Step 4 – Export to wide CSV  (one row per payment, one column per tag)
  Step 5 – Parse an MX pacs.008 message  (new ISO 20022 XML format)
  Step 6 – Show MT103 → MX field mapping side by side
  Step 7 – Export MX to CSV

Run with:
    python main.py
Or with custom input files:
    python main.py --mt103-file path/to/batch.fin --mx-file path/to/msg.xml
"""

import argparse
import sys
from pathlib import Path

# ── Local modules ────────────────────────────────────────────────────────────
from mt103 import (
    read_mt103_batch,
    parse_swift_mt_to_dict,
    enrich_with_descriptions,
    to_mt103_message,
    write_mt103_wide_csv,
)
from mx_pacs008 import parse_pacs008, write_pacs008_csv

# ── File paths ────────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent
DEFAULT_MT103_FILE = _HERE / "input" / "mt103_transactions.fin"
DEFAULT_MX_FILE    = _HERE / "input" / "mx_pacs008_example.xml"
DEFAULT_MT103_CSV  = _HERE / "output" / "mt103_wide_batch.csv"
DEFAULT_MX_CSV     = _HERE / "output" / "mx_pacs008_output.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _banner(title: str) -> None:
    """Print a section separator for easier reading."""
    width = 70
    print()
    print("═" * width)
    print(f"  {title}")
    print("═" * width)


def _sub(title: str) -> None:
    print(f"\n── {title} " + "─" * max(0, 65 - len(title)))


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------

def run(mt103_file: Path, mx_file: Path, mt103_csv: Path, mx_csv: Path) -> None:

    # ════════════════════════════════════════════════════════════════════════
    # PART 1 – MT103  (legacy SWIFT FIN format)
    # ════════════════════════════════════════════════════════════════════════
    _banner("PART 1 — MT103 (Legacy SWIFT FIN / plain-text format)")

    # ── Step 1: Read batch file ──────────────────────────────────────────
    _sub("Step 1 · Read batch .fin file")
    print(f"  Input : {mt103_file}")
    raw_messages = read_mt103_batch(str(mt103_file))
    print(f"  Found : {len(raw_messages)} MT103 message(s)")

    # ── Step 2: Parse + enrich ───────────────────────────────────────────
    _sub("Step 2 · Parse blocks & fields, enrich with descriptions")
    enriched_list = []
    typed_list    = []

    for i, raw in enumerate(raw_messages, start=1):
        parsed   = parse_swift_mt_to_dict(raw)
        enriched = enrich_with_descriptions(parsed)
        typed    = to_mt103_message(enriched)
        enriched_list.append(enriched)
        typed_list.append(typed)

    print(f"  Parsed {len(enriched_list)} message(s) successfully.")

    # ── Step 3: Print per-payment summary ───────────────────────────────
    _sub("Step 3 · Payment summaries")
    for i, typed in enumerate(typed_list, start=1):
        print(f"\n  [{i}] {typed.summary()}")

        # Show the SWIFT block breakdown for the first message only
        if i == 1:
            print()
            print("       Block breakdown (first message):")
            from mt103 import BLOCK_DESCRIPTIONS
            for b_id, desc in BLOCK_DESCRIPTIONS.items():
                val = typed.block_1 if b_id == "1" else (
                      typed.block_2 if b_id == "2" else (
                      typed.block_3 if b_id == "3" else (
                      typed.block_5 if b_id == "5" else "…see CSV…")))
                short_val = str(val)[:50] + "…" if val and len(str(val)) > 50 else str(val)
                print(f"         {{{b_id}:}} {desc.split('–')[0].strip():<30} → {short_val}")

            print()
            print("       Field breakdown (first message, Block 4):")
            fields_meta = enriched_list[0]["text_block"].get("fields_with_meta", {})
            for tag, meta in fields_meta.items():
                val_short = str(meta['value']).replace("\n", " / ")[:45]
                print(f"         :{tag:<4}: {meta['description']:<45} = {val_short}")

    # ── Step 4: Export to wide CSV ───────────────────────────────────────
    _sub("Step 4 · Export to wide CSV (one row per payment)")
    write_mt103_wide_csv(enriched_list, str(mt103_csv))
    print(f"  ✅  Written {len(enriched_list)} row(s) → {mt103_csv}")
    print("       Columns: message_id | mt_type | block_1..5 | f_20_… | f_32A_… | …")

    # ════════════════════════════════════════════════════════════════════════
    # PART 2 – pacs.008  (new ISO 20022 / MX XML format)
    # ════════════════════════════════════════════════════════════════════════
    _banner("PART 2 — pacs.008 (New ISO 20022 / MX XML format)")

    # ── Step 5: Parse MX message ─────────────────────────────────────────
    _sub("Step 5 · Parse pacs.008 XML message")
    print(f"  Input : {mx_file}")
    transactions = parse_pacs008(mx_file)
    print(f"  Found : {len(transactions)} transaction(s)\n")

    for i, tx in enumerate(transactions, start=1):
        print(f"  [{i}] {tx.summary()}")

    # ── Step 6: Show MT103 ↔ MX field mapping ────────────────────────────
    _sub("Step 6 · MT103 → MX field mapping (side-by-side)")
    for tx in transactions:
        print(tx.mt103_comparison())

    # ── Step 7: Export to CSV ────────────────────────────────────────────
    _sub("Step 7 · Export MX to CSV")
    write_pacs008_csv(transactions, str(mx_csv))
    print(f"  ✅  Written {len(transactions)} row(s) → {mx_csv}")

    # ════════════════════════════════════════════════════════════════════════
    # Summary
    # ════════════════════════════════════════════════════════════════════════
    _banner("DONE — Output files")
    print(f"  {mt103_csv}")
    print(f"  {mx_csv}")
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="SWIFT MT103 + ISO 20022 pacs.008 parsing demo"
    )
    p.add_argument(
        "--mt103-file", type=Path, default=DEFAULT_MT103_FILE,
        help="Path to the MT103 batch .fin file  (default: input/mt103_transactions.fin)",
    )
    p.add_argument(
        "--mx-file", type=Path, default=DEFAULT_MX_FILE,
        help="Path to the pacs.008 XML file  (default: input/mx_pacs008_example.xml)",
    )
    p.add_argument(
        "--mt103-csv", type=Path, default=DEFAULT_MT103_CSV,
        help="Output CSV for MT103  (default: mt103_wide_batch.csv)",
    )
    p.add_argument(
        "--mx-csv", type=Path, default=DEFAULT_MX_CSV,
        help="Output CSV for pacs.008  (default: mx_pacs008_output.csv)",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    # Validate inputs exist
    for fpath in (args.mt103_file, args.mx_file):
        if not fpath.exists():
            print(f"ERROR: input file not found: {fpath}", file=sys.stderr)
            sys.exit(1)

    run(args.mt103_file, args.mx_file, args.mt103_csv, args.mx_csv)
