# Databricks notebook source
# SETUP: create the target schema once, before the parallel Bronze tasks start.
# Idempotent: safe to run on every pipeline run.

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("schema", "airflow_medallion")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("airflow_run_id", "manual")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")  # fails fast if the catalog is missing or not accessible
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
    COMMENT 'Medallion demo (bronze/silver/gold), managed by Airflow DAG medallion_pipeline'
""")

print(f"[setup] schema {catalog}.{schema} is ready")
dbutils.notebook.exit(f"{catalog}.{schema}")
