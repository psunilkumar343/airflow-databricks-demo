# Databricks DAG-Based Orchestration with Airflow

Airflow defines the **DAG**: which steps run, in what order, on what schedule, and how failures are retried.
**Databricks** runs every step. Each Airflow task submits a Databricks notebook run and waits for it to finish.

```
start
  -> create_schema                                  (CREATE SCHEMA IF NOT EXISTS dev.airflow_medallion)
  -> bronze [orders | customers | nation]           (3 Databricks runs in parallel)
  -> silver [transform]                             (clean + join + quality checks)
  -> gold   [revenue_by_country | top_customers]    (2 Databricks runs in parallel)
  -> end
```

| Layer  | Notebook | Tables | What happens |
|--------|----------|--------|--------------|
| Setup  | `00_setup` | - | Creates the schema if it doesn't exist yet, so reruns are safe |
| Bronze | `01_bronze_ingest`   | `bronze_orders` (7.5M rows), `bronze_customers`, `bronze_nation` | Raw copy of `samples.tpch`, plus `_ingested_at`, `_run_date` and `_airflow_run_id` columns |
| Silver | `02_silver_transform`| `silver_customers`, `silver_orders` | Readable column names, correct types, customers joined to their country, duplicates removed. **Quality checks** run here; if one fails, Silver fails and Gold doesn't run. |
| Gold   | `03_gold_aggregate`  | `gold_revenue_by_country`, `gold_top_customers` | Revenue by country and year, and the top 100 customers |

## Orchestration features
| Feature | Setting |
|---|---|
| Dependencies | `start >> create_schema >> bronze >> silver >> gold >> end`. If a step fails, everything after it is skipped. |
| Parallel Databricks runs | The tasks are deferrable: each one submits its run, then waits in Airflow's triggerer, so the tasks inside a group run at the same time in Databricks |
| Schedule | Every day at 06:00 IST (`0 6 * * *`). `catchup=False`, so missed days aren't run automatically. Only one run at a time. |
| Retries | 3 retries, waiting longer each time (1m, 2m, 4m, up to 10m). Databricks API calls also retry 5 times. |
| Timeouts | 30 min per task and 1 h per whole run |
| Parameters | Each notebook receives `catalog`, `schema`, `run_date` (`{{ ds }}`) and `airflow_run_id` |
| Compute | Serverless by default. Set `CLUSTER_ID` in `.env` to use an existing cluster. |

## Project layout
```
dags/medallion_pipeline_dag.py   the Airflow DAG
notebooks/                       the 4 Databricks notebooks (imported into the Databricks workspace)
docker-compose.yml               local Airflow 2.10.5
.env / .env.example              settings: Databricks host/token, notebook folder, catalog/schema, Airflow login
archive/                         the earlier hello-world demo (Airflow doesn't load it)
```

## Setup
1. **Import the notebooks.** In Databricks, import the 4 files in `notebooks/` into one workspace folder.
2. **Settings.** Copy `.env.example` to `.env`. Fill in `DATABRICKS_HOST`, `DATABRICKS_TOKEN` and `NOTEBOOK_DIR` (the folder from step 1), plus `AIRFLOW_ADMIN_PASSWORD`.
3. **Start Airflow.** Run `docker compose up -d` and wait about a minute. Then open http://localhost:8080 and log in with `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from `.env`.
4. **Run it.** Unpause `medallion_pipeline`. It then runs every day at 06:00 IST, or you can click **Trigger DAG** to run it now.

Stop Airflow with `docker compose down`. Your Databricks tables aren't affected, but Airflow's run history is cleared.

Requirements: a Unity Catalog workspace with the `samples` catalog. The token's user needs `USE CATALOG` and `CREATE SCHEMA` on `dev`.

## Demo script
1. **Graph view:** walk through the DAG. Bronze, Silver and Gold are groups; the tasks inside a group run in parallel.
2. **Trigger** the DAG and watch the groups turn green in order.
3. **Task > Logs:** shows the Databricks run URL. Click it to open the same run in Databricks.
4. **Databricks > Job Runs:** each Airflow task appears as a run named `airflow__<notebook>__<task>__<date>`.
5. **Catalog Explorer** (`dev.airflow_medallion`): look at bronze, then silver, then gold.
   ```sql
   SELECT * FROM dev.airflow_medallion.gold_revenue_by_country ORDER BY revenue DESC;
   SELECT * FROM dev.airflow_medallion.gold_top_customers ORDER BY rank LIMIT 10;
   ```
6. **Failure handling:** rename a notebook in Databricks and trigger. The task retries 3 times, then fails, and the steps after it are skipped.
7. **Schedule:** point out "Next Run" (06:00 IST) in the DAG list.

## Alternative: trigger an existing Databricks Job
If the pipeline is already set up as a Databricks Job, use `DatabricksRunNowOperator(job_id=...)` instead. See `archive/dags/greet_job_dag.py`.
