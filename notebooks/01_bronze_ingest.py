# Databricks notebook source
# BRONZE: land the raw source table as-is, plus ingestion metadata.
# Source: Databricks built-in dataset samples.tpch (available in every Unity Catalog workspace).

dbutils.widgets.text("source", "orders")          # orders | customers | nation
dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("schema", "airflow_medallion")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("airflow_run_id", "manual")

source = dbutils.widgets.get("source")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_date = dbutils.widgets.get("run_date")
run_id = dbutils.widgets.get("airflow_run_id")

SOURCES = {
    "orders": "samples.tpch.orders",
    "customers": "samples.tpch.customer",
    "nation": "samples.tpch.nation",
}
if source not in SOURCES:
    raise ValueError(f"Unknown source '{source}'. Expected one of {list(SOURCES)}")

target = f"{catalog}.{schema}.bronze_{source}"
# Schema is created by the create_schema task (00_setup) before this runs.

# COMMAND ----------

from pyspark.sql import functions as F

df = (
    spark.table(SOURCES[source])
    .withColumn("_source_table", F.lit(SOURCES[source]))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_run_date", F.lit(run_date))
    .withColumn("_airflow_run_id", F.lit(run_id))
)

# Full snapshot load: re-running the same day gives the same result (idempotent).
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)

# COMMAND ----------

rows = spark.table(target).count()
print(f"[bronze] {SOURCES[source]} -> {target}: {rows:,} rows (run_date={run_date})")
if rows == 0:
    raise RuntimeError(f"{target} is empty - failing so Airflow retries / alerts")

dbutils.notebook.exit(f"{target}:{rows}")
