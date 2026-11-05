# Databricks notebook source
# GOLD: business-ready tables built from Silver. One notebook, one report per run
# (Airflow runs the reports in parallel).

dbutils.widgets.text("report", "revenue_by_country")   # revenue_by_country | top_customers
dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("schema", "airflow_medallion")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("airflow_run_id", "manual")

report = dbutils.widgets.get("report")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_date = dbutils.widgets.get("run_date")
db = f"{catalog}.{schema}"

# COMMAND ----------

REPORTS = {
    # Revenue, order count and average order value per country and year
    "revenue_by_country": f"""
        SELECT country,
               order_year,
               COUNT(*)                          AS orders,
               COUNT(DISTINCT customer_id)       AS customers,
               CAST(SUM(order_amount) AS DECIMAL(18,2)) AS revenue,
               CAST(AVG(order_amount) AS DECIMAL(14,2)) AS avg_order_value,
               '{run_date}'                      AS _run_date
        FROM {db}.silver_orders
        GROUP BY country, order_year
    """,
    # Top 100 customers by lifetime revenue, ranked
    "top_customers": f"""
        SELECT RANK() OVER (ORDER BY lifetime_revenue DESC) AS rank, *
        FROM (
            SELECT customer_id,
                   customer_name,
                   country,
                   market_segment,
                   COUNT(*)                                  AS orders,
                   CAST(SUM(order_amount) AS DECIMAL(18,2))  AS lifetime_revenue,
                   MIN(order_date)                           AS first_order,
                   MAX(order_date)                           AS last_order,
                   '{run_date}'                              AS _run_date
            FROM {db}.silver_orders
            GROUP BY customer_id, customer_name, country, market_segment
        )
        QUALIFY RANK() OVER (ORDER BY lifetime_revenue DESC) <= 100
    """,
}

if report not in REPORTS:
    raise ValueError(f"Unknown report '{report}'. Expected one of {list(REPORTS)}")

target = f"{db}.gold_{report}"
spark.sql(f"CREATE OR REPLACE TABLE {target} AS {REPORTS[report]}")

# COMMAND ----------

result = spark.table(target)
print(f"[gold] {target}: {result.count():,} rows (run_date={run_date})")
display(result.limit(20))
