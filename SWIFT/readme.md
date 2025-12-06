# MT103 SWIFT Parser (Python)

This repository contains a small Python toolkit to parse **SWIFT MT103** messages and export them to **CSV** in a “wide” analytical format (one row per payment, one column per tag).

The goal is to give data engineers a minimal but clear example of:

- Splitting raw SWIFT FIN files into individual MT103 messages  
- Parsing blocks and fields (`:20:`, `:23B:`, `:32A:`, …)  
- Enriching tags with human-readable descriptions  
- Writing a wide CSV suitable for analytics / DWH

> ⚠️ This is **educational/demo code**, not a full SWIFT-certified parser.  
> Don’t use it as-is for production payments processing without proper validation and testing.

---

## MT103 in a nutshell

A SWIFT MT103 (Customer Credit Transfer) is a text message with 5 blocks:

- `{1:}` Basic header  
- `{2:}` Application header  
- `{3:}` User header (optional)  
- `{4:}` **Text block** – the business fields (`:20:`, `:32A:`, `:50K:`, `:59:`, etc.)  
- `{5:}` Trailer

Inside block 4, MT fields look like:

```text
:20:20250101-ABC123
:23B:CRED
:32A:250101EUR1234,56
:50K:/DE44500105175407324931
JOHN DOE
MAIN STREET 1
DE-10115 BERLIN
:59:/US12300078901234567890
JANE SMITH
123 5TH AVENUE
NEW YORK NY 10001
:71A:SHA
