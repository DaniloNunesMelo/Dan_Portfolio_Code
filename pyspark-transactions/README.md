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

# run with a custom config
python3.12 -m e3_contracts_to_transactions.main \
  --contracts data/contracts.csv \
  --claims    data/claims.csv \
  --config    config/parameters.yaml \
  --output    output/TRANSACTIONS.csv
```

---

## Project layout

```
├── config/
│   └── parameters.yaml        # Externalised business rules (see below)
├── e3_contracts_to_transactions/
│   ├── __init__.py
│   ├── main.py                # CLI entrypoint & orchestration
│   ├── config.py              # YAML config loader & validation
│   ├── transform.py           # Pure Spark transformations (no I/O, no side-effects)
│   ├── api.py                 # Hashify API client (injectable)
│   ├── io_utils.py            # CSV read / write helpers
│   └── schemas.py             # Shared StructType definitions
├── tests/
│   ├── __init__.py
│   ├── conftest.py            # Session-scoped Spark fixture & default config
│   ├── test_transform.py      # Unit tests for every mapping rule (31 tests)
│   ├── test_api.py            # API client tests – mocked HTTP (5 tests)
│   ├── test_config.py         # Config loader tests (6 tests)
│   ├── test_io_utils.py       # CSV read/write tests (14 tests)
│   └── test_main.py           # CLI & orchestration tests (13 tests)
├── data/
│   ├── contracts.csv          # Sample input data
│   └── claims.csv             # Sample input data
├── requirements.txt
├── requirements-dev.txt
└── README.md
```

---

## Pipeline parameters

All business rules live in `config/parameters.yaml` so they can change
**without touching any code**.  The file is loaded at startup by `config.py`,
which also validates that every required key is present.

### Current parameters

| Parameter | Purpose | Example value |
|-----------|---------|---------------|
| `source_system` | Literal written to `CONTRACT_SOURCE_SYSTEM` | `"Europe 3"` |
| `transaction_type_mapping` | Maps `CLAIM_TYPE` → label | `{"1": "Private", "2": "Corporate"}` |
| `transaction_type_default` | Fallback when type is null / unknown | `"Unknown"` |
| `transaction_direction_mapping` | Maps `CLAIM_ID` prefix → direction | `{"CL": "COINSURANCE", "RX": "REINSURANCE"}` |
| `date_of_loss_format` | Input date format for `DATE_OF_LOSS` | `"dd.MM.yyyy"` |
| `creation_date_format` | Input datetime format for `CREATION_DATE` | `"dd.MM.yyyy HH:mm"` |
| `hashify_base_url` | Hashify API endpoint | `"https://api.hashify.net/hash/md4/hex"` |
| `hashify_response_field` | JSON field to extract from API response | `"Digest"` |
| `claim_contract_join` | Column names used in the claim → contract left join | see YAML |
| `output_header` | Include CSV header row | `true` |
| `output_delimiter` | CSV column separator | `","` |

### Adding a new transaction type

Edit `parameters.yaml` — no code change, no redeployment of logic:

```yaml
transaction_type_mapping:
  "1": "Private"
  "2": "Corporate"
  "3": "SME"
  "4": "Government"
  "5": "NonProfit"
  "6": "GroupPolicy"
  "7": "BrokerSubmitted"
  "8": "PartnerNetwork"
  "9": "Reinsurance"
  "10": "Internal"

```

### Overriding the config path

The config file can be specified in three ways (highest priority first):

1. CLI flag: `--config /path/to/parameters.yaml`
2. Environment variable: `PIPELINE_CONFIG=/path/to/parameters.yaml`
3. Default: `config/parameters.yaml` relative to the project root

---

## Towards a database-backed parameter store

The current YAML file is convenient for a single pipeline, but in a
production environment with multiple source systems and teams, some or all
of these parameters could be migrated to a database (or a configuration
management service).

### Which parameters are candidates for a database?

| Parameter | Why it belongs in a DB |
|-----------|----------------------|
| `transaction_type_mapping` | Shared across pipelines; changes driven by business analysts who shouldn't edit YAML. A `lookup_transaction_type` table (`claim_type_code`, `label`, `effective_from`, `effective_to`) supports versioning and auditing. |
| `transaction_direction_mapping` | Same reasoning — a `lookup_direction` table (`prefix`, `direction`, `active`) lets operations enable or disable prefixes without deployments. |
| `source_system` | When onboarding a new source (e.g. "Europe 4"), the label is just master data — a row in a `source_systems` table. |
| `hashify_base_url` | Environment-specific (dev / staging / prod); better suited to a secrets manager or a `pipeline_config` table keyed by environment. |

### What should stay in a file (or env vars)?

Technical parameters that are tightly coupled to the code version are
better kept close to the code:

- `date_of_loss_format`, `creation_date_format` — tied to the source
  extract format; changes here usually need code-level regression tests.
- `claim_contract_join` column names — structural; a change implies a
  schema migration.
- `output_header`, `output_delimiter` — rarely change; environment
  variables or a file are sufficient.

### Proposed architecture

```
┌──────────────┐      ┌─────────────────┐      ┌───────────────┐
│  parameters  │      │  DB / config    │      │   Pipeline    │
│  .yaml       │─────▶│  service        │─────▶│   config.py   │
│  (static)    │      │  (dynamic)      │      │   (merged)    │
└──────────────┘      └─────────────────┘      └───────────────┘
```

1. **`config.py` loads the YAML first** (file-based defaults).
2. **Then queries the database** for any overrides keyed by
   `(source_system, environment)`.
3. **DB values win** — the merged dict is what the pipeline uses.
4. **Fallback**: if the DB is unreachable, the pipeline can still run
   with YAML-only values (degraded mode), or fail fast depending on policy.

### Example database tables

```sql
-- Lookup table for transaction types (versioned)
CREATE TABLE lookup_transaction_type (
    id              SERIAL PRIMARY KEY,
    source_system   TEXT NOT NULL,
    claim_type_code TEXT NOT NULL,
    label           TEXT NOT NULL,
    effective_from  DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_to    DATE,
    created_by      TEXT NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Lookup table for direction prefixes
CREATE TABLE lookup_direction (
    id              SERIAL PRIMARY KEY,
    source_system   TEXT NOT NULL,
    prefix          TEXT NOT NULL,
    direction       TEXT NOT NULL,
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- General pipeline config (key-value, environment-aware)
CREATE TABLE pipeline_config (
    source_system   TEXT NOT NULL,
    environment     TEXT NOT NULL,       -- dev / staging / prod
    key             TEXT NOT NULL,
    value           TEXT NOT NULL,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_system, environment, key)
);
```

### Implementation sketch

The existing `config.py` would gain an optional `_load_from_db` step:

```python
def load_parameters(path=None, db_conn=None, env="prod"):
    config = _load_yaml(path)                 # static defaults

    if db_conn:
        source = config["source_system"]
        config["transaction_type_mapping"] = _query_type_mapping(db_conn, source)
        config["transaction_direction_mapping"] = _query_direction_mapping(db_conn, source)
        # override any key-value pairs from pipeline_config
        for key, value in _query_kv(db_conn, source, env):
            config[key] = value

    _validate(config)
    return config
```

The rest of the pipeline (`transform.py`, `main.py`) stays unchanged because
it already consumes a plain `dict` — it does not care where the values came from.

---

## Column mappings

| Target column              | Source / logic                                                       |
|----------------------------|----------------------------------------------------------------------|
| CONTRACT_SOURCE_SYSTEM     | Literal from `config.source_system` (default `"Europe 3"`)          |
| CONTRACT_SOURCE_SYSTEM_ID  | `CONTRACT.CONTRACT_ID` via left-join (null when no match)            |
| SOURCE_SYSTEM_ID           | Numeric suffix of `CLAIM_ID` (e.g. `CL_68545123` → `68545123`)      |
| TRANSACTION_TYPE           | Driven by `config.transaction_type_mapping` and `transaction_type_default` |
| TRANSACTION_DIRECTION      | Driven by `config.transaction_direction_mapping` (prefix → direction) |
| CONFORMED_VALUE            | `CLAIM.AMOUNT` cast to `decimal(16,5)`                               |
| BUSINESS_DATE              | `DATE_OF_LOSS` parsed using `config.date_of_loss_format`             |
| CREATION_DATE              | `CREATION_DATE` parsed using `config.creation_date_format`           |
| SYSTEM_TIMESTAMP           | `current_timestamp()` at transformation time                         |
| NSE_ID                     | MD4 hex digest of `CLAIM_ID` via Hashify API                         |

---

## Handling new batches (incremental loads)

To ensure that existing transactions created in previous batches are not
overwritten, we propose an **upsert (merge)** strategy:

1. **Primary key**: use the composite key defined in the output schema
   (`CONTRACT_SOURCE_SYSTEM`, `CONTRACT_SOURCE_SYSTEM_ID`, `NSE_ID`).
2. **On each batch run**, produce a staging DataFrame with the new
   transactions.
3. **Merge into the target**: if the target is a Delta Lake table or a
   database, use `MERGE INTO` (or `INSERT ... ON CONFLICT`) to insert new
   rows and optionally update changed rows while leaving untouched rows
   intact.
4. **For CSV targets**: read the existing file, anti-join on the PK to
   find genuinely new rows, union, and rewrite. (Less efficient, but
   works without a database.)
5. **Audit columns**: `SYSTEM_TIMESTAMP` already records when each row was
   created. Adding a `BATCH_ID` column (e.g. a run UUID or date-stamp)
   would further support lineage and debugging.

---

## Design decisions

### Testability

Every transformation rule is a **small, pure function** that takes and returns
a DataFrame. Tests build DataFrames in-memory (no file I/O) with explicit mock
data that covers each edge-case independently. The test suite has **69 tests**
covering transforms, API, config loading, I/O, and end-to-end orchestration.

### Externalised configuration

Business rules (type mappings, direction mappings, source system label) live in
`config/parameters.yaml`, not in code. This lets business analysts request
changes via a config update rather than a code deployment. The section above
describes how these parameters can migrate to a database as the platform matures.

### Dependency injection

The API client is injected as a callable `(str) -> str`. Tests pass a
deterministic lambda; production passes the real HTTP function. No monkey-patching needed.

### Separation of concerns

- `schemas.py` – schema definitions only
- `config.py` – parameter loading & validation only
- `io_utils.py` – file reads and writes only
- `transform.py` – pure Spark column logic only (no I/O, no HTTP)
- `api.py` – HTTP client only
- `main.py` – wiring / orchestration only
