"""
### Databricks DAG-based orchestration: Bronze -> Silver -> Gold

    start
      -> create_schema                                   (CREATE SCHEMA IF NOT EXISTS)
      -> bronze  [orders | customers | nation]           (parallel Databricks runs)
      -> silver  [transform]                             (clean, join, quality checks)
      -> gold    [revenue_by_country | top_customers]    (parallel Databricks runs)
      -> end

**Schedule:** daily 06:00 IST. **Retries:** 3 with exponential backoff.
Airflow only orchestrates: every task submits a Databricks notebook run and waits for it.
"""
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.task_group import TaskGroup

# ---- Config (from .env, see docker-compose.yml) -------------------------------
NOTEBOOK_DIR = os.environ.get("NOTEBOOK_DIR", "/Workspace/Shared/airflow_medallion").rstrip("/")
CLUSTER_ID = os.environ.get("CLUSTER_ID", "").strip()  # empty = serverless
CATALOG = os.environ.get("CATALOG", "dev")
SCHEMA = os.environ.get("SCHEMA", "airflow_medallion")

LOCAL_TZ = pendulum.timezone("Asia/Kolkata")
SCHEDULE = "0 6 * * *"  # every day at 06:00 IST

# Parameters every notebook receives. {{ ds }} / {{ run_id }} are rendered by Airflow.
COMMON_PARAMS = {
    "catalog": CATALOG,
    "schema": SCHEMA,
    "run_date": "{{ ds }}",
    "airflow_run_id": "{{ run_id }}",
}


def databricks_notebook(task_id: str, notebook: str, **params) -> DatabricksSubmitRunOperator:
    """One Airflow task = one Databricks notebook run (serverless unless CLUSTER_ID is set)."""
    task_spec = {
        "task_key": task_id,
        "notebook_task": {
            "notebook_path": f"{NOTEBOOK_DIR}/{notebook}",
            "base_parameters": {**COMMON_PARAMS, **params},
        },
        "timeout_seconds": 1800,  # Databricks-side timeout for the notebook run
    }
    if CLUSTER_ID:
        task_spec["existing_cluster_id"] = CLUSTER_ID

    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id="databricks_default",
        json={"run_name": f"airflow__{notebook}__{task_id}__{{{{ ds }}}}", "tasks": [task_spec]},
        # Submit the run, then wait in Airflow's triggerer: parallel tasks run in
        # Databricks at the same time even though local Airflow runs one task at a time.
        deferrable=True,
        polling_period_seconds=10,
        databricks_retry_limit=5,  # retries on Databricks API errors / rate limits
        databricks_retry_delay=10,
    )


with DAG(
    dag_id="medallion_pipeline",
    description="Databricks DAG-based orchestration: Bronze -> Silver -> Gold",
    start_date=pendulum.datetime(2026, 11, 1, tz=LOCAL_TZ),
    schedule=SCHEDULE,
    catchup=False,                     # don't run missed past days automatically
    max_active_runs=1,                 # never two pipeline runs at the same time
    dagrun_timeout=timedelta(hours=1),
    default_args={
        "owner": "data-engineering",
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,   # 1m, 2m, 4m ...
        "max_retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["databricks", "medallion"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Setup: create the target schema once (idempotent), before the parallel Bronze tasks
    create_schema = databricks_notebook("create_schema", "00_setup")

    with TaskGroup("bronze", tooltip="Raw copies of the source tables + ingestion metadata") as bronze:
        for src in ("orders", "customers", "nation"):
            databricks_notebook(src, "01_bronze_ingest", source=src)

    with TaskGroup("silver", tooltip="Cleaned, typed, joined + data-quality checks") as silver:
        databricks_notebook("transform", "02_silver_transform")

    with TaskGroup("gold", tooltip="Business-ready aggregates") as gold:
        for report in ("revenue_by_country", "top_customers"):
            databricks_notebook(report, "03_gold_aggregate", report=report)

    start >> create_schema >> bronze >> silver >> gold >> end
