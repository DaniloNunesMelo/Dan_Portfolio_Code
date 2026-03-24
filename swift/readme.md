# SWIFT Payment Parsing — MT103 & ISO 20022 (MX pacs.008)

Educational Python toolkit covering both generations of SWIFT payment messaging:

- **MT103** — the legacy SWIFT FIN (plain-text) format in use since the 1990s
- **pacs.008** — the new ISO 20022 / MX (XML) format replacing MT globally

The goal is to give data engineers and fintech learners a minimal, well-commented
example of how international payment messages are structured, parsed, and exported
to analytical CSV.

> ⚠️ **Educational/demo code.** Not a certified SWIFT parser.
> Do not use for production payment processing without proper validation and testing.

---

## Project structure

```
swift/
├── input/                    # Raw input files (committed to repo)
│   ├── mt103_transactions.fin    # Sample batch of 5 MT103 messages
│   └── mx_pacs008_example.xml    # Annotated example pacs.008 message
├── output/                   # Generated CSVs (gitignored, created at runtime)
│   ├── mt103_wide_batch.csv
│   └── mx_pacs008_output.csv
├── artifacts/                # CI/CD deposits files here for upload & repo commit
├── mt103.py                  # MT103 parser, enrichment, CSV export
├── mx_pacs008.py             # ISO 20022 pacs.008 XML parser, CSV export
├── main.py                   # Didactic entry point — runs the full workflow
├── requirements.txt          # pytest, pytest-cov, ruff (zero runtime deps)
├── conftest.py               # pytest sys.path setup
└── tests/
    ├── test_mt103.py         # 44 unit tests for the MT103 parser
    └── test_mx_pacs008.py    # 54 unit tests for the pacs.008 parser
```

---

## Quick start

```bash
# 1. Install dev dependencies (runtime needs only stdlib)
pip install -r requirements.txt

# 2. Run the full didactic workflow
python main.py

# 3. Run only the MT103 parser
python mt103.py input/mt103_transactions.fin        # → output/mt103_wide_batch.csv

# 4. Run only the MX pacs.008 parser
python mx_pacs008.py input/mx_pacs008_example.xml  # → output/mx_pacs008_output.csv

# 5. Run tests
pytest tests/ -v
```

---

## Part 1 — MT103 (Legacy SWIFT FIN format)

### What is MT103?

MT103 is a **SWIFT Customer Credit Transfer** message — the standard way banks
have instructed international wire transfers since the 1990s. "MT" stands for
**Message Type**; the format is plain text (not XML).

### Message structure

Every MT message is divided into numbered **blocks**:

| Block | Name | Contains |
|-------|------|----------|
| `{1:}` | Basic Header | Sender BIC, session & sequence number |
| `{2:}` | Application Header | Message type (103), receiver BIC |
| `{3:}` | User Header | Optional overrides and references |
| `{4:}` | **Text Block** | All business payment fields (`:20:`, `:32A:`, `:50K:`, `:59:`, …) |
| `{5:}` | Trailer | Checksums (CHK) and integrity flags |

Inside Block 4, each field starts with a `:TAG:` marker:

```text
{1:F01BANKDEFFXXXX1234567890}{2:I103BANKUS33XXXXN}{4:
:20:20250101-ABC123
:23B:CRED
:32A:250101EUR1234,56
:50K:/DE44500105175407324931
JOHN DOE
MAIN STREET 1
DE-10115 BERLIN
:57A:BANKUS33XXX
:59:/US12300078901234567890
JANE SMITH
123 5TH AVENUE
NEW YORK NY 10001
:70:INVOICE 2024-0001 / CONSULTING SERVICES
:71A:SHA
:77B:/REG/TRADE PAYMENT
-}{5:{CHK:AABBCC112233}}
```

### Key MT103 fields

| Tag | Description | Example value |
|-----|-------------|---------------|
| `:20:` | Transaction Reference Number | `20250101-ABC123` |
| `:23B:` | Bank Operation Code | `CRED` (always for MT103) |
| `:32A:` | Value Date / Currency / Amount | `250101EUR1234,56` |
| `:33B:` | Original Ordered Amount (before FX) | `EUR1234,56` |
| `:36:` | Exchange Rate | `1,087` |
| `:50K:` | Ordering Customer (sender) | IBAN + name + address |
| `:53B:` | Sender's Correspondent (intermediary bank) | `/3000123456` |
| `:57A:` | Account With Institution (beneficiary's bank) | `BANKUS33XXX` |
| `:59:` | Beneficiary Customer (receiver) | IBAN + name + address |
| `:70:` | Remittance Information | Invoice number / free text |
| `:71A:` | Details of Charges | `SHA` / `OUR` / `BEN` |
| `:77B:` | Regulatory Reporting | AML / compliance codes |

### Using the MT103 module

```python
from mt103 import read_mt103_batch, parse_swift_mt_to_dict, enrich_with_descriptions, to_mt103_message, write_mt103_wide_csv

# 1. Read a .fin batch file
raw_messages = read_mt103_batch("input/mt103_transactions.fin")

# 2. Parse each message
for raw in raw_messages:
    parsed   = parse_swift_mt_to_dict(raw)    # → nested dict (blocks + fields)
    enriched = enrich_with_descriptions(parsed) # → adds human-readable labels
    typed    = to_mt103_message(enriched)       # → MT103Message dataclass

    print(typed.summary())
    # [20250101-ABC123] 250101EUR1234,56 | Sender: /DE44500... → Benef: /US12300... | Charges: SHA

    # Access fields directly
    print(typed.ref)              # "20250101-ABC123"
    print(typed.charges)          # "SHA"
    print(typed.remittance_info)  # "INVOICE 2024-0001 / CONSULTING SERVICES"

# 3. Export all to wide CSV
write_mt103_wide_csv(
    [enrich_with_descriptions(parse_swift_mt_to_dict(r)) for r in raw_messages],
    "mt103_wide_batch.csv",
)
```

**Wide CSV output** — one row per payment, one column per tag:

```
message_id | mt_type | block_1 | … | f_20_Transaction_Reference_Number | f_32A_Value_Date_… | f_50K_Ordering_Customer | …
1          | 103     | F01BANK | … | 20250101-ABC123                   | 250101EUR1234,56   | /DE445…JOHN DOE…       | …
```

---

## Part 2 — ISO 20022 / MX pacs.008 (New standard)

### What is ISO 20022?

ISO 20022 is the **new global standard** for financial messaging, replacing SWIFT's
legacy MT format. SWIFT calls these messages **MX** (as opposed to MT).

Key differences from MT:

| Feature | MT103 (legacy) | pacs.008 (ISO 20022) |
|---------|---------------|----------------------|
| Format | Plain text | Structured XML |
| Ambiguity | Free-text addresses | Structured name/address fields |
| Transaction tracking | No universal ID | **UETR** — UUID4 tracker across all banks |
| Standard | SWIFT proprietary | ISO open standard (SEPA, TARGET2, FedNow, …) |
| Address fields | Unstructured lines | Street, postcode, town, country |
| Purpose code | `:77B:` free text | Standardised `Purp/Cd` (e.g. `SALA`, `COSU`) |

### MT103 → pacs.008 field mapping

| MT103 tag | pacs.008 XML element | Notes |
|-----------|----------------------|-------|
| `:20:` | `CdtTrfTxInf/PmtId/TxId` | Transaction reference |
| *(none)* | `CdtTrfTxInf/PmtId/UETR` | **New** — universal end-to-end tracker |
| `:32A:` date | `GrpHdr/IntrBkSttlmDt` | Settlement date |
| `:32A:` amount | `CdtTrfTxInf/IntrBkSttlmAmt @Ccy` | Amount + currency attribute |
| `:33B:` | `CdtTrfTxInf/InstdAmt @Ccy` | Original instructed amount |
| `:50K:` | `CdtTrfTxInf/Dbtr` + `DbtrAcct` | Debtor name + IBAN |
| `:53B:` | `CdtTrfTxInf/DbtrAgt/FinInstnId/BICFI` | Debtor's bank BIC |
| `:57A:` | `CdtTrfTxInf/CdtrAgt/FinInstnId/BICFI` | Creditor's bank BIC |
| `:59:` | `CdtTrfTxInf/Cdtr` + `CdtrAcct` | Creditor name + IBAN |
| `:70:` | `CdtTrfTxInf/RmtInf/Ustrd` | Remittance information |
| `:71A:` | `CdtTrfTxInf/ChrgBr` | `SHAR`=SHA / `DEBT`=OUR / `CRED`=BEN |
| `:77B:` | `CdtTrfTxInf/Purp/Cd` | Standardised purpose code |

### pacs.008 message structure

```xml
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08">
  <FIToFICstmrCdtTrf>

    <!-- Group Header: applies to all transactions in the message -->
    <GrpHdr>
      <MsgId>MSG-20250101-001</MsgId>           <!-- message-level ID -->
      <CreDtTm>2025-01-01T10:00:00</CreDtTm>
      <NbOfTxs>1</NbOfTxs>
      <TtlIntrBkSttlmAmt Ccy="EUR">1234.56</TtlIntrBkSttlmAmt>
      <IntrBkSttlmDt>2025-01-01</IntrBkSttlmDt>  <!-- ← :32A: date -->
      <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>
    </GrpHdr>

    <!-- One CdtTrfTxInf per payment transaction -->
    <CdtTrfTxInf>
      <PmtId>
        <TxId>20250101-ABC123</TxId>                       <!-- ← :20: -->
        <UETR>550e8400-e29b-41d4-a716-446655440000</UETR>  <!-- NEW: no MT equiv. -->
      </PmtId>
      <IntrBkSttlmAmt Ccy="EUR">1234.56</IntrBkSttlmAmt>   <!-- ← :32A: amount -->
      <ChrgBr>SHAR</ChrgBr>                                  <!-- ← :71A: SHA -->
      <DbtrAgt><FinInstnId><BICFI>BANKDEFFXXX</BICFI></FinInstnId></DbtrAgt>  <!-- ← :53B: -->
      <Dbtr><Nm>John Doe</Nm></Dbtr>                         <!-- ← :50K: name -->
      <DbtrAcct><Id><IBAN>DE44500105175407324931</IBAN></Id></DbtrAcct>
      <CdtrAgt><FinInstnId><BICFI>BANKUS33XXX</BICFI></FinInstnId></CdtrAgt>  <!-- ← :57A: -->
      <Cdtr><Nm>Jane Smith</Nm></Cdtr>                       <!-- ← :59: name -->
      <CdtrAcct><Id><Othr><Id>US12300078901234567890</Id></Othr></Id></CdtrAcct>
      <Purp><Cd>COSU</Cd></Purp>                             <!-- ← :77B: -->
      <RmtInf><Ustrd>INVOICE 2024-0001 / CONSULTING SERVICES</Ustrd></RmtInf>  <!-- ← :70: -->
    </CdtTrfTxInf>

  </FIToFICstmrCdtTrf>
</Document>
```

### Using the pacs.008 module

```python
from mx_pacs008 import parse_pacs008, write_pacs008_csv

# 1. Parse from file or XML string
transactions = parse_pacs008("input/mx_pacs008_example.xml")

# 2. Access fields directly
tx = transactions[0]
print(tx.summary())
# [20250101-ABC123] EUR 1234.56 | Debtor: John Doe → Creditor: Jane Smith | UETR: 550e8400-…

print(tx.tx_id)           # "20250101-ABC123"
print(tx.uetr)            # "550e8400-e29b-41d4-a716-446655440000"
print(tx.debtor_name)     # "John Doe"
print(tx.debtor_iban)     # "DE44500105175407324931"
print(tx.creditor_bic)    # "BANKUS33XXX"
print(tx.charge_bearer)   # "SHAR"

# 3. Print the MT103 → MX mapping side by side
print(tx.mt103_comparison())

# 4. Export to CSV
write_pacs008_csv(transactions, "mx_pacs008_output.csv")
```

**CSV output** columns are named after the pacs.008 element path with the MT103
equivalent noted in the source code:

```
msg_id | created_at | settlement_date | currency | amount | tx_id | uetr | charge_bearer | debtor_name | debtor_iban | debtor_bic | creditor_bic | creditor_name | creditor_iban | remittance_info | purpose_code
```

---

## Running the tests

```bash
# All tests (98 total)
pytest tests/ -v

# With coverage report
pytest tests/ -v --cov=. --cov-omit="tests/*,conftest.py" --cov-report=term-missing

# MT103 tests only
pytest tests/test_mt103.py -v

# MX tests only
pytest tests/test_mx_pacs008.py -v
```

Tests use **synthetic (hand-crafted) data** — no real payment data is needed.
Each test method is annotated to explain what SWIFT concept it is verifying.

---

## CI/CD (GitHub Actions)

The `.github/workflows/` directory contains four scoped pipelines:

| Workflow | Trigger | What it does |
|----------|---------|-------------|
| `ci-swift.yml` | Push/PR on `swift/**` | ruff lint + format check + pytest with coverage (Python 3.11, 3.12, 3.13) |
| `cd-swift.yml` | Push to `master` on `swift/**` | Runs `main.py`, uploads CSV artifacts, commits sample output |
| `ci-pyspark.yml` | Push/PR on `pyspark-transactions/**` | PySpark test suite |
| `cd-pyspark.yml` | Push to `master` on `pyspark-transactions/**` | PySpark pipeline run |

Each workflow is **path-scoped** — changing a file in `swift/` never triggers
the PySpark pipeline, and vice versa.

---

## Sample data

### MT103 batch (`input/mt103_transactions.fin`)

Five international payments covering different currency pairs and charge structures:

| # | Corridor | Currency | Amount | Charges |
|---|----------|----------|--------|---------|
| 1 | Germany → USA | EUR | 1 234.56 | SHA |
| 2 | Switzerland → UK | CHF | 9 876.50 | OUR |
| 3 | France → Italy | USD | 2 500.00 (EUR 2 300.00) | SHA + FX |
| 4 | Spain → Netherlands | GBP | 15 000.75 | BEN |
| 5 | Portugal → Brazil | BRL | 7 890.90 (USD 1 500.00) | SHA + FX |

### MX pacs.008 (`input/mx_pacs008_example.xml`)

One transaction — the same Germany → USA payment as MT103 message #1 — so
you can compare the two formats field by field.

---

> ⚠️ This is **educational/demo code**, not a full SWIFT-certified parser.
> Do not use for production payment processing without proper validation and testing.
