"""Daily lakehouse run, one task per pipeline stage.

Each task calls the same CLI used locally, so the DAG holds no business logic. The task
graph mirrors the data dependencies:

    bronze -> silver_reference --+--> silver_transactions -> gold
           -> silver_customers --+

The reference and customer silver loads are independent and run in parallel. Every task is
idempotent for a given {{ ds }}, so retries and backfills (`airflow dags backfill`) are safe.
On AWS, swap the BashOperator for GlueJobOperator (see glue_jobs/) and nothing else changes.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator

PIPELINE = "python -m lakehouse.pipeline"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="banking_lakehouse_daily",
    description="Bronze -> Silver -> Gold for retail banking transactions",
    start_date=datetime(2026, 9, 1),
    schedule="0 2 * * *",  # 02:00 UTC, after the upstream feeds land
    catchup=False,
    max_active_runs=1,  # SCD2 and cross-batch dedupe depend on the previous day
    default_args=default_args,
    tags=["lakehouse", "pyspark", "banking"],
) as dag:

    def stage(name: str) -> BashOperator:
        return BashOperator(
            task_id=name,
            bash_command=f"{PIPELINE} --run-date {{{{ ds }}}} --stage {name} --batch-id {{{{ run_id }}}}",
            env={"LAKEHOUSE_BASE_PATH": "{{ var.value.get('lakehouse_base_path', 'data') }}"},
            append_env=True,
        )

    bronze = stage("bronze")
    silver_reference = stage("silver_reference")
    silver_customers = stage("silver_customers")
    silver_transactions = stage("silver_transactions")
    gold = stage("gold")

    bronze >> [silver_reference, silver_customers]
    silver_reference >> silver_transactions
    [silver_transactions, silver_customers] >> gold
