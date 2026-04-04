"""Airflow DAG factory — Contracts-to-Transactions pipeline (multi-region).

One independent DAG is created per region. Each DAG can be triggered,
paused, and monitored separately in the Airflow UI.

Schedule : daily at 02:00 UTC for all regions
Tasks    : validate_input >> run_etl >> validate_output

Assets & dependency graph
---------------------------
Each region's ``run_etl`` task declares an outlet ``Asset`` whose URI is
the absolute path to the output TRANSACTIONS.csv file.  A separate
``global_transactions_aggregation`` DAG is scheduled on *all* region
datasets — it starts automatically once every region has produced fresh
output and is visible in the Airflow **Asset Dependencies** graph.

Adding a new region
-------------------
Add a single entry to ``REGION_CONFIGS`` — no other code change required:

    {
        "region_id": "latin_america_1",
        "display_name": "Latin America 1",
        "config":    "config/latin_america_1/parameters.yaml",
        "contracts": "data/latin_america_1/contracts.csv",
        "claims":    "data/latin_america_1/claims.csv",
        "output":    "output/latin_america_1/TRANSACTIONS.csv",
    }
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

from airflow import DAG
from airflow.sdk import Asset
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

_JOB_SCRIPT = str(_PROJECT_ROOT / "jobs" / "pipeline.py")

# ---------------------------------------------------------------------------
# Region registry — add one dict per region to onboard it
# ---------------------------------------------------------------------------
REGION_CONFIGS: list[dict[str, str]] = [
    {
        "region_id":    "europe_3",
        "display_name": "Europe 3",
        "config":       "config/europe_3/parameters.yaml",
        "contracts":    "data/europe_3/contracts.csv",
        "claims":       "data/europe_3/claims.csv",
        "output":       "output/europe_3/TRANSACTIONS.csv",
    },
    {
        "region_id":    "asia_pacific_1",
        "display_name": "Asia Pacific 1",
        "config":       "config/asia_pacific_1/parameters.yaml",
        "contracts":    "data/asia_pacific_1/contracts.csv",
        "claims":       "data/asia_pacific_1/claims.csv",
        "output":       "output/asia_pacific_1/TRANSACTIONS.csv",
    },
    {
        "region_id":    "north_america_2",
        "display_name": "North America 2",
        "config":       "config/north_america_2/parameters.yaml",
        "contracts":    "data/north_america_2/contracts.csv",
        "claims":       "data/north_america_2/claims.csv",
        "output":       "output/north_america_2/TRANSACTIONS.csv",
    },
]

# Assets per region — source inputs (inlets) and output (outlet).
# Airflow uses these to draw the Asset Dependencies graph:
#   contracts + claims  →  run_etl  →  TRANSACTIONS
# and to trigger the aggregation DAG once every region has updated.
REGION_DATASETS: dict[str, dict[str, Asset]] = {
    region["region_id"]: {
        "contracts": Asset(f"file://{_PROJECT_ROOT / region['contracts']}"),
        "claims":    Asset(f"file://{_PROJECT_ROOT / region['claims']}"),
        "output":    Asset(f"file://{_PROJECT_ROOT / region['output']}"),
    }
    for region in REGION_CONFIGS
}

# ---------------------------------------------------------------------------
# Default arguments shared across all region DAGs
# ---------------------------------------------------------------------------
_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


# ---------------------------------------------------------------------------
# DAG factory
# ---------------------------------------------------------------------------

def make_region_dag(region: dict[str, str]) -> DAG:
    """Build and return an independent Airflow DAG for one region.

    GX imports are deferred inside callables so the scheduler can parse
    this file without great-expectations installed in its environment.

    The ``run_etl`` task declares ``outlets`` so that Airflow marks the
    region's Asset as updated on every successful run.  This feeds the
    Asset Dependencies graph and triggers the aggregation DAG.
    """
    region_id    = region["region_id"]
    display_name = region["display_name"]
    config_path  = str(_PROJECT_ROOT / region["config"])
    contracts    = str(_PROJECT_ROOT / region["contracts"])
    claims       = str(_PROJECT_ROOT / region["claims"])
    output       = str(_PROJECT_ROOT / region["output"])
    ds_contracts = REGION_DATASETS[region_id]["contracts"]
    ds_claims    = REGION_DATASETS[region_id]["claims"]
    ds_output    = REGION_DATASETS[region_id]["output"]

    def _validate_input(**_context: Any) -> None:
        from contracts_to_transactions.gx_validation import validate_claims
        validate_claims(claims)

    def _validate_output(**_context: Any) -> None:
        from contracts_to_transactions.gx_validation import validate_transactions
        validate_transactions(output)

    with DAG(
        dag_id=f"{region_id}_contracts_to_transactions",
        default_args=_DEFAULT_ARGS,
        description=f"{display_name} — Contracts to Transactions ETL",
        schedule="0 2 * * *",
        catchup=False,
        tags=[region_id, "etl", "pyspark"],
    ) as dag:

        validate_input = PythonOperator(
            task_id="validate_input",
            python_callable=_validate_input,
            inlets=[ds_claims],            # claims.csv appears as source node
        )

        run_etl = SparkSubmitOperator(
            task_id="run_etl",
            application=_JOB_SCRIPT,
            application_args=[
                "--contracts", contracts,
                "--claims",    claims,
                "--output",    output,
                "--config",    config_path,
            ],
            name=f"{region_id}_contracts_to_transactions",
            conn_id="spark_local",
            driver_memory="4g",
            verbose=True,
            inlets=[ds_contracts, ds_claims],  # both source files visible in graph
            outlets=[ds_output],               # TRANSACTIONS.csv marked updated on success
        )

        validate_output = PythonOperator(
            task_id="validate_output",
            python_callable=_validate_output,
        )

        validate_input >> run_etl >> validate_output

    return dag


# ---------------------------------------------------------------------------
# Register one DAG per region — Airflow discovers globals() of type DAG
# ---------------------------------------------------------------------------
for _region in REGION_CONFIGS:
    _dag = make_region_dag(_region)
    globals()[_dag.dag_id] = _dag


# ---------------------------------------------------------------------------
# Aggregation DAG — scheduled on all region datasets
#
# Airflow starts this DAG automatically once *every* region dataset has been
# updated in the same scheduling window.  It appears as a downstream node in
# the Asset Dependencies graph for each region.
# ---------------------------------------------------------------------------
_all_datasets = [ds["output"] for ds in REGION_DATASETS.values()]

with DAG(
    dag_id="global_transactions_aggregation",
    default_args=_DEFAULT_ARGS,
    description="Cross-region aggregation — runs after all regions complete",
    schedule=_all_datasets,   # data-aware schedule: triggers on all datasets
    catchup=False,
    tags=["global", "aggregation", "pyspark"],
) as global_transactions_aggregation:

    def _aggregate(**_) -> None:
        """Placeholder for cross-region aggregation logic."""
        import logging
        log = logging.getLogger(__name__)
        regions = [r["display_name"] for r in REGION_CONFIGS]
        log.info("All regions completed: %s", regions)
        log.info("Cross-region aggregation triggered by dataset updates.")

    aggregate = PythonOperator(
        task_id="aggregate_all_regions",
        python_callable=_aggregate,
    )
