# Databricks notebook source
# SILVER: clean, rename to business-friendly columns, deduplicate, join reference data,
# then run data-quality checks. A failed check fails the task -> Airflow retries and
# skips Gold, so bad data never reaches the business tables.

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("schema", "airflow_medallion")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("airflow_run_id", "manual")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_date = dbutils.widgets.get("run_date")
db = f"{catalog}.{schema}"

# COMMAND ----------

# Customers enriched with country (nation) and region
spark.sql(f"""
CREATE OR REPLACE TABLE {db}.silver_customers AS
SELECT DISTINCT
    CAST(c.c_custkey AS BIGINT)        AS customer_id,
    TRIM(c.c_name)                     AS customer_name,
    INITCAP(TRIM(n.n_name))            AS country,
    TRIM(c.c_mktsegment)               AS market_segment,
    CAST(c.c_acctbal AS DECIMAL(12,2)) AS account_balance,
    '{run_date}'                       AS _run_date
FROM {db}.bronze_customers c
LEFT JOIN {db}.bronze_nation n ON c.c_nationkey = n.n_nationkey
WHERE c.c_custkey IS NOT NULL
""")

# COMMAND ----------

# Orders: typed, readable status, joined to customer
spark.sql(f"""
CREATE OR REPLACE TABLE {db}.silver_orders AS
SELECT
    CAST(o.o_orderkey AS BIGINT)          AS order_id,
    CAST(o.o_custkey AS BIGINT)           AS customer_id,
    c.customer_name,
    c.country,
    c.market_segment,
    CAST(o.o_orderdate AS DATE)           AS order_date,
    YEAR(o.o_orderdate)                   AS order_year,
    CASE o.o_orderstatus
        WHEN 'F' THEN 'Fulfilled'
        WHEN 'O' THEN 'Open'
        WHEN 'P' THEN 'Partial'
        ELSE 'Unknown'
    END                                   AS order_status,
    TRIM(o.o_orderpriority)               AS priority,
    CAST(o.o_totalprice AS DECIMAL(14,2)) AS order_amount,
    '{run_date}'                          AS _run_date
FROM {db}.bronze_orders o
JOIN {db}.silver_customers c ON o.o_custkey = c.customer_id
WHERE o.o_orderkey IS NOT NULL
  AND o.o_totalprice > 0
""")

# COMMAND ----------

# Data-quality checks
checks = spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {db}.silver_customers)                                  AS customers,
  (SELECT COUNT(*) FROM {db}.silver_orders)                                     AS orders,
  (SELECT COUNT(*) FROM {db}.bronze_orders)                                     AS bronze_orders,
  (SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM {db}.silver_orders)          AS duplicate_orders,
  (SELECT COUNT(*) FROM {db}.silver_customers WHERE country IS NULL)            AS customers_without_country
""").first().asDict()

print("[silver] quality metrics:", checks)

failures = []
if checks["orders"] == 0:
    failures.append("silver_orders is empty")
if checks["duplicate_orders"] > 0:
    failures.append(f"{checks['duplicate_orders']} duplicate order_ids")
if checks["customers_without_country"] > 0:
    failures.append(f"{checks['customers_without_country']} customers without country")
if checks["orders"] < 0.99 * checks["bronze_orders"]:
    failures.append(f"more than 1% of orders dropped ({checks['orders']} of {checks['bronze_orders']})")

if failures:
    raise AssertionError("Data quality checks failed: " + "; ".join(failures))

print("[silver] all quality checks passed")
dbutils.notebook.exit(f"customers={checks['customers']}, orders={checks['orders']}")
