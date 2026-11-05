# Databricks notebook source
# Creates a small sample "bronze" table. Airflow passes the parameters below.
dbutils.widgets.text("source", "orders")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "airflow_demo")
dbutils.widgets.text("run_date", "")

source = dbutils.widgets.get("source")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_date = dbutils.widgets.get("run_date")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

if source == "orders":
    spark.sql(f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.bronze_orders AS
        SELECT id AS order_id,
               CAST(id % 5 AS INT) + 1 AS customer_id,
               ROUND(10 + rand() * 90, 2) AS amount,
               '{run_date}' AS load_date
        FROM range(1, 101)
    """)
elif source == "customers":
    spark.sql(f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.bronze_customers AS
        SELECT id AS customer_id,
               concat('Customer ', id) AS name,
               element_at(array('UAE','India','UK','USA','Germany'), CAST(id AS INT)) AS country,
               '{run_date}' AS load_date
        FROM range(1, 6)
    """)
else:
    raise ValueError(f"Unknown source: {source}")

print(f"Loaded bronze_{source} for {run_date}")
