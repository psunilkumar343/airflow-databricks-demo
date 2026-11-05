# Databricks notebook source
# Joins orders with customers. Runs only after BOTH ingests succeed.
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "airflow_demo")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}.silver_orders AS
    SELECT o.order_id, o.amount, o.load_date, c.customer_id, c.name, c.country
    FROM {catalog}.{schema}.bronze_orders o
    JOIN {catalog}.{schema}.bronze_customers c USING (customer_id)
""")

# COMMAND ----------

print(spark.table(f"{catalog}.{schema}.silver_orders").count(), "rows in silver_orders")
