# Contracts to Transactions – Multi-Region Pipeline

**PySpark pipeline** that reads CONTRACT and CLAIM CSV extracts from multiple
regional source systems and produces a TRANSACTIONS CSV per region in a
standardised target schema.

Orchestrated by **Apache Airflow** (daily schedule, one independent DAG per
region) with **Great Expectations** quality gates before and after each Spark
job. Adding a new region requires only a config file, a data directory, and
one line in the DAG factory's `REGION_CONFIGS` list.

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
pip3.12 install -r requirements-dev.txt

# Install the package as editable so Airflow workers can import it
pip3.12 install --no-build-isolation -e .

# run tests
pytest -v
python3.12 -m pytest -v
python3.12 -m pytest -v --log-file=output/test.log --log-file-level=INFO

# This captures everything (test results + logs) into one file
python3.12 -m pytest -v 2>&1 | tee output/test.log

# run any region directly (without Airflow)
python3.12 -m contracts_to_transactions.main \
  --contracts data/europe_3/contracts.csv \
  --claims    data/europe_3/claims.csv \
  --config    config/europe_3/parameters.yaml \
  --output    output/europe_3/TRANSACTIONS.csv

python3.12 -m contracts_to_transactions.main \
  --contracts data/asia_pacific_1/contracts.csv \
  --claims    data/asia_pacific_1/claims.csv \
  --config    config/asia_pacific_1/parameters.yaml \
  --output    output/asia_pacific_1/TRANSACTIONS.csv

python3.12 -m contracts_to_transactions.main \
  --contracts data/north_america_2/contracts.csv \
  --claims    data/north_america_2/claims.csv \
  --config    config/north_america_2/parameters.yaml \
  --output    output/north_america_2/TRANSACTIONS.csv
```

### Running via Airflow

```bash
# 1. Configure the Spark connection in Airflow
#    Admin → Connections → spark_default (type: Spark, host: spark://localhost:7077)
#    For local mode leave the host field empty.

# 2. Point Airflow at the dags/ folder (or copy the DAG file there)
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags

# 3. Trigger any region independently
airflow dags trigger europe_3_contracts_to_transactions
airflow dags trigger asia_pacific_1_contracts_to_transactions
airflow dags trigger north_america_2_contracts_to_transactions
```

The DAG factory in `dags/pipeline_dag.py` registers one independent DAG per
region. Each DAG runs three tasks in sequence:

```
validate_input  →  run_etl  →  validate_output
  (GX / pandas)    (Spark)      (GX / pandas)
```

### Running GX validation independently

```bash
python -c "
from e3_contracts_to_transactions.gx_validation import validate_claims, validate_transactions
validate_claims('data/claims.csv')
validate_transactions('output/TRANSACTIONS.csv')
"
```

---

## Project layout

```
├── config/
│   ├── europe_3/
│   │   └── parameters.yaml        # Europe 3 rules (dd.MM.yyyy dates)
│   ├── asia_pacific_1/
│   │   └── parameters.yaml        # Asia Pacific 1 rules (yyyy-MM-dd dates)
│   └── north_america_2/
│       └── parameters.yaml        # North America 2 rules (MM/dd/yyyy dates)
├── dags/
│   └── pipeline_dag.py            # DAG factory — one Airflow DAG per region
├── jobs/
│   └── pipeline.py                # SparkSubmitOperator entrypoint (thin wrapper)
├── contracts_to_transactions/     # Region-agnostic processing package
│   ├── __init__.py
│   ├── main.py                    # CLI entrypoint & orchestration
│   ├── config.py                  # YAML config loader & validation
│   ├── transform.py               # Pure Spark transformations (no I/O, no side-effects)
│   ├── validate.py                # Runtime Spark data quality guard
│   ├── gx_validation.py           # Great Expectations orchestration-level quality gates
│   ├── api.py                     # Hashify API client (injectable)
│   ├── io_utils.py                # CSV read / write helpers
│   └── schemas.py                 # Shared StructType definitions
├── tests/
│   ├── __init__.py
│   ├── conftest.py                # Session-scoped Spark fixture & default config
│   ├── test_transform.py          # Unit tests for every mapping rule (31 tests)
│   ├── test_api.py                # API client tests – mocked HTTP (5 tests)
│   ├── test_config.py             # Config loader tests (6 tests)
│   ├── test_io_utils.py           # CSV read/write tests (14 tests)
│   └── test_main.py               # CLI & orchestration tests (13 tests)
├── data/
│   ├── europe_3/
│   │   ├── contracts.csv          # Europe 3 sample input
│   │   └── claims.csv
│   ├── asia_pacific_1/
│   │   ├── contracts.csv          # Asia Pacific 1 synthetic data (ISO dates)
│   │   └── claims.csv
│   └── north_america_2/
│       ├── contracts.csv          # North America 2 synthetic data (US dates)
│       └── claims.csv
├── output/
│   ├── europe_3/
│   ├── asia_pacific_1/
│   └── north_america_2/
├── requirements.txt
├── requirements-dev.txt
└── README.md
```

---

## Multi-region architecture

The processing package (`contracts_to_transactions/`) contains no region-specific logic. Every business rule is externalised to a per-region `config/{region}/parameters.yaml`. Region differences that would otherwise require code changes are handled by config alone:

| Parameter | Europe 3 | Asia Pacific 1 | North America 2 |
|---|---|---|---|
| `source_system` | `"Europe 3"` | `"Asia Pacific 1"` | `"North America 2"` |
| `date_of_loss_format` | `dd.MM.yyyy` | `yyyy-MM-dd` | `MM/dd/yyyy` |
| `creation_date_format` | `dd.MM.yyyy HH:mm` | `yyyy-MM-dd HH:mm` | `MM/dd/yyyy HH:mm` |

Airflow runs each region as a **fully independent DAG** generated by the factory in `dags/pipeline_dag.py`. Pausing, rerunning, or adjusting retries for one region does not affect others.

### Adding a new region

1. Create `config/{region}/parameters.yaml` with region-specific values
2. Add `data/{region}/contracts.csv` and `data/{region}/claims.csv`
3. Add one dict to `REGION_CONFIGS` in `dags/pipeline_dag.py`:

```python
{
    "region_id":    "latin_america_1",
    "display_name": "Latin America 1",
    "config":       "config/latin_america_1/parameters.yaml",
    "contracts":    "data/latin_america_1/contracts.csv",
    "claims":       "data/latin_america_1/claims.csv",
    "output":       "output/latin_america_1/TRANSACTIONS.csv",
}
```

No other code change is required.

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

## Data quality architecture

Two complementary validation layers run for every pipeline execution:

| Layer | File | When | How | Surface |
|---|---|---|---|---|
| **Runtime Spark guard** | `validate.py` | Inside the Spark job | Spark DataFrame counts | `RuntimeError` aborts the job |
| **Orchestration GX gate** | `gx_validation.py` | Before & after Spark via Airflow | pandas + Great Expectations | `ValidationError` → Airflow task FAILED |

### Pre-ETL expectations (`validate_claims`)

| Expectation | Column | Reason |
|---|---|---|
| Column exists | all 8 source columns | Catches upstream schema drift before Spark launches |
| Not null | `CLAIM_ID`, `AMOUNT` | Null CLAIM_ID corrupts NSE_ID hash; null AMOUNT silently maps to CHARGE |
| Unique | `CLAIM_ID` | Duplicate CLAIM_ID → duplicate NSE_ID in output |
| Regex `^[A-Z]{2}_\d+$` | `CLAIM_ID` | Malformed prefix → null TRANSACTION_DIRECTION; malformed suffix → null SOURCE_SYSTEM_ID |

### Post-ETL expectations (`validate_transactions`)

| Expectation | Column | Reason |
|---|---|---|
| Column exists | all 11 output columns | Guards against TRANSACTION_CATEGORY being dropped by select() |
| Not null + unique | `NSE_ID` | Primary identifier for downstream consumers |
| In set | `TRANSACTION_TYPE` | Mapping regression (new CLAIM_TYPE in data but not in YAML) → silent "Unknown" |
| In set (nulls allowed) | `TRANSACTION_DIRECTION` | Unknown prefixes produce null (valid per schema) but unexpected values surface here |
| In set | `TRANSACTION_CATEGORY` | Config label rename would silently break downstream |

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
| TRANSACTION_CATEGORY       | `CHARGE` (positive `CONFORMED_VALUE`) or `REFUND` (negative)         |

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
- `validate.py` – runtime Spark data quality guard only
- `gx_validation.py` – orchestration-level GX quality gates only (pandas, no Spark)
- `api.py` – HTTP client only
- `main.py` – wiring / orchestration only
- `jobs/pipeline.py` – SparkSubmitOperator entrypoint only
- `dags/pipeline_dag.py` – Airflow DAG factory (one DAG per region)
