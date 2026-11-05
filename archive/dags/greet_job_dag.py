from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

JOB_ID = 532333682461264


def greet(task_id, text):
    return DatabricksRunNowOperator(
        task_id=task_id,
        databricks_conn_id="databricks_default",
        job_id=JOB_ID,
        notebook_params={"greeting": text},
    )


with DAG(
    "databricks_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"owner": "airflow", "retries": 1},
) as dag:

    greet_india = greet("greet_india", "India")
    greet_uae = greet("greet_uae", "UAE")
    summary = greet("summary", "Both done")

    [greet_india, greet_uae] >> summary