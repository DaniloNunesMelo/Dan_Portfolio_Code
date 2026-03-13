# Contracts to Transactions – Europe 3

**PySpark pipeline** that reads CONTRACT and CLAIM CSV extracts from the
"Europe 3" source and produces a TRANSACTIONS CSV in the target schema.

---

## Quick start


```bash
# Install JAVA
sudo apt install openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip3.12 install -r requirements.txt

# run tests
pytest -v
python3.12 -m pytest -v
python3.12 -m pytest -v --log-file=output/test.log --log-file-level=INFO

# This captures everything (test results + logs) into one file
python3.12 -m pytest -v 2>&1 | tee output/test.log

# run pipeline
python3.12 -m e3_contracts_to_transactions.main \
  --contracts data/contracts.csv \
  --claims    data/claims.csv \
  --output    output/TRANSACTIONS.csv
```

---

## Project layout

```
├── e3_contracts_to_transactions/
│   ├── __init__.py
│   ├── main.py            # CLI entrypoint & orchestration
│   ├── transform.py       # Pure Spark transformations (no I/O, no side-effects)
│   ├── api.py             # Hashify API client (injectable)
│   ├── io_utils.py        # CSV read / write helpers
│   └── schemas.py         # Shared StructType definitions
├── tests/
│   ├── __init__.py
│   ├── conftest.py        # Session-scoped Spark fixture
│   ├── test_transform.py  # Unit tests for every mapping rule
│   └── test_api.py        # API client tests (mocked HTTP)
├── data/
│   ├── contracts.csv      # Mock test data (comprehensive edge-cases)
│   └── claims.csv         # Mock test data (comprehensive edge-cases)
├── requirements.txt
└── README.md
```

---

## Column mappings

| Target column              | Source / logic                                                       |
|----------------------------|----------------------------------------------------------------------|
| CONTRACT_SOURCE_SYSTEM     | Literal `"Europe 3"`                                                 |
| CONTRACT_SOURCE_SYSTEM_ID  | `CONTRACT.CONTRACT_ID` via left-join (null when no match)            |
| SOURCE_SYSTEM_ID           | Numeric suffix of `CLAIM_ID` (e.g. `CL_68545123` → `68545123`)      |
| TRANSACTION_TYPE           | `Corporate` if claim_type=2, `Private` if claim_type=1, else `Unknown` |
| TRANSACTION_DIRECTION      | `COINSURANCE` if CL prefix, `REINSURANCE` if RX prefix, else null   |
| CONFORMED_VALUE            | `CLAIM.AMOUNT` cast to `decimal(16,5)`                               |
| BUSINESS_DATE              | `DATE_OF_LOSS` parsed from `dd.MM.yyyy`                              |
| CREATION_DATE              | `CREATION_DATE` parsed from `dd.MM.yyyy HH:mm`                      |
| SYSTEM_TIMESTAMP           | `current_timestamp()` at transformation time                         |
| NSE_ID                     | MD4 hex digest of `CLAIM_ID` via Hashify API                         |

---

## Design decisions

### Testability

Every transformation rule is a **small, pure function** that takes and returns
a DataFrame. Tests build DataFrames in-memory (no file I/O) with explicit mock
data that covers each edge-case independently.

### Dependency injection

The API client is injected as a callable `(str) -> str`. Tests pass a
deterministic lambda; production passes the real HTTP function. No monkey-patching needed.

### Separation of concerns

- `schemas.py` – schema definitions only
- `io_utils.py` – file reads and writes only
- `transform.py` – pure Spark column logic only (no I/O, no HTTP)
- `api.py` – HTTP client only
- `main.py` – wiring / orchestration only
