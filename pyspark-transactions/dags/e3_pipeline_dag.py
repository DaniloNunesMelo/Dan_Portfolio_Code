"""Airflow DAG — Europe 3 Contracts-to-Transactions pipeline.

Schedule : daily at 02:00 UTC
Tasks    : validate_input >> run_etl >> validate_output

``validate_input`` and ``validate_output`` run Great Expectations checks via
PythonOperator (pandas datasource, ephemeral GX context, no Spark required).

``run_etl`` submits the Spark job via SparkSubmitOperator pointing at
``jobs/e3_pipeline.py``.  It passes the same file paths used by the
validation tasks so all three tasks operate on the same data.

GX imports are deferred inside the Python callables so the Airflow scheduler
can parse this DAG file without great-expectations installed in the
scheduler's environment — only the worker that executes the task needs it.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ---------------------------------------------------------------------------
# Path constants
# Resolved relative to this file so the DAG works when the project directory
# is mounted into an Airflow container.  In production replace with absolute
# paths or Airflow Variables (Variable.get("E3_PROJECT_ROOT")).
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONTRACTS_PATH = str(_PROJECT_ROOT / "data" / "contracts.csv")
CLAIMS_PATH    = str(_PROJECT_ROOT / "data" / "claims.csv")
OUTPUT_PATH    = str(_PROJECT_ROOT / "output" / "TRANSACTIONS.csv")
CONFIG_PATH    = str(_PROJECT_ROOT / "config" / "parameters.yaml")
JOB_SCRIPT     = str(_PROJECT_ROOT / "jobs" / "e3_pipeline.py")

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

# ---------------------------------------------------------------------------
# Python callables (deferred imports)
# ---------------------------------------------------------------------------

def _validate_input(**_context: object) -> None:
    """GX pre-ETL check: validate raw claims CSV."""
    from e3_contracts_to_transactions.gx_validation import validate_claims

    validate_claims(CLAIMS_PATH)


def _validate_output(**_context: object) -> None:
    """GX post-ETL check: validate generated TRANSACTIONS CSV."""
    from e3_contracts_to_transactions.gx_validation import validate_transactions

    validate_transactions(OUTPUT_PATH)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="e3_contracts_to_transactions",
    default_args=default_args,
    description="Europe 3 — Contracts to Transactions ETL with GX quality gates",
    schedule="0 2 * * *",
    catchup=False,
    tags=["europe3", "etl", "pyspark"],
) as dag:

    validate_input = PythonOperator(
        task_id="validate_input",
        python_callable=_validate_input,
    )

    run_etl = SparkSubmitOperator(
        task_id="run_etl",
        application=JOB_SCRIPT,
        application_args=[
            "--contracts", CONTRACTS_PATH,
            "--claims",    CLAIMS_PATH,
            "--output",    OUTPUT_PATH,
            "--config",    CONFIG_PATH,
        ],
        name="e3_contracts_to_transactions",
        # conn_id references the Airflow Spark connection configured in
        # Admin → Connections (type: Spark, host: spark://localhost:7077
        # for a standalone cluster, or leave host empty for local mode).
        conn_id="spark_default",
        num_executors=4,
        executor_memory="16g",
        driver_memory="4g",
        verbose=True,
    )

    validate_output = PythonOperator(
        task_id="validate_output",
        python_callable=_validate_output,
    )

    validate_input >> run_etl >> validate_output
