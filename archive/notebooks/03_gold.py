# Databricks notebook source
# Revenue per country.
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "airflow_demo")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}.gold_revenue_by_country AS
    SELECT country, COUNT(*) AS orders, ROUND(SUM(amount), 2) AS revenue
    FROM {catalog}.{schema}.silver_orders
    GROUP BY country
""")

display(spark.table(f"{catalog}.{schema}.gold_revenue_by_country"))
