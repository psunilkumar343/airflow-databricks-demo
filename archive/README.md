# Databricks DAG Orchestration with Airflow – Demo

## Airflow in 1 minute
- **DAG** = a Python file that describes tasks and their order (`a >> b` means "b runs after a").
- **Operator** = one kind of task. Here we use `DatabricksSubmitRunOperator` to run a Databricks notebook.
- **Scheduler** decides when tasks run, retries failures, and skips downstream tasks if one fails.
- **Connection** = stored Databricks host + token, so DAG code holds no secrets.
- Airflow only *orchestrates*. All data processing runs in Databricks.

## What the demo does
```
start -> [ingest_orders, ingest_customers] (parallel) -> silver -> gold -> end
```
Each box is a Databricks notebook run. Silver waits for both ingests; gold waits for silver.
Airflow passes `catalog`, `schema` and `run_date` ({{ ds }}) to the notebooks.

## Setup (10 min)
1. In Databricks, import the 3 files in `notebooks/` into one workspace folder (Workspace > Import).
2. Create a Databricks token (User Settings > Developer > Access tokens).
3. `cp .env.example .env` and fill in host, token, notebook folder (CLUSTER_ID empty = serverless).
4. `docker compose up` and wait ~1 min. Open http://localhost:8080. Login `admin`; the password is printed in the container logs (or in `simple_auth_manager_passwords.json.generated`).
5. Unpause `databricks_medallion_demo`, click **Trigger**.

## What to show your manager
- **Graph** tab: the DAG structure, tasks turning green in order.
- Click a task > **Logs**: contains the Databricks run URL, which opens the run in Databricks.
- Databricks: tables `main.airflow_demo.gold_revenue_by_country` etc.
- Retries: break a notebook path, trigger, and see retry then failure; downstream tasks are skipped.

## Alternative (from the Databricks doc)
`DatabricksRunNowOperator(job_id=...)` triggers an *existing* Databricks Job instead of submitting the notebook directly. The doc recommends it when jobs are already defined in Databricks. Swap the operator and pass `job_id`.
