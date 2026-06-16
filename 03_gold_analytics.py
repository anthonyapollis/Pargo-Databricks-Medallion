# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold Layer — Business-Ready Analytics
# MAGIC
# MAGIC **Medallion layer:** Gold
# MAGIC **Source:** `pargo_silver.*`
# MAGIC **Output:** `pargo_gold.*` — aggregated, business-ready Delta tables
# MAGIC
# MAGIC | Table | Grain | Business use |
# MAGIC |-------|-------|-------------|
# MAGIC | `parcel_status_daily` | date × status | Daily status breakdown for ops dashboard |
# MAGIC | `rts_analysis` | retailer × province × month | RTS rate by retailer — cost saving target |
# MAGIC | `hub_throughput_daily` | hub × date | Hub capacity and scan volume |
# MAGIC | `courier_performance` | courier × month | Delivery success rate per courier |
# MAGIC | `exec_summary_monthly` | month | Executive KPI summary |

# COMMAND ----------

from pyspark.sql import functions as F

parcels = spark.table('pargo_silver.parcels')
events  = spark.table('pargo_silver.tracking_events')
orders  = spark.table('pargo_silver.orders')

# Drop columns from orders that also exist in parcels to avoid ambiguous references after join
_orders_join = orders.drop('retailer_id', 'customer_id')

# COMMAND ----------

# ── 1. Parcel status daily ───────────────────────────────────────────────────
parcel_status_daily = (
    parcels
    .filter(F.col('created_date').isNotNull())
    .groupBy('created_date', 'status')
    .agg(
        F.count('*').alias('parcel_count'),
        F.countDistinct('pickup_point_id').alias('pickup_points_active'),
        F.avg('weight_kg').alias('avg_weight_kg'),
        F.avg('collection_attempts').alias('avg_collection_attempts')
    )
    .withColumn('_gold_loaded_at', F.current_timestamp())
    .orderBy('created_date', 'status')
)

parcel_status_daily.write.format('delta').mode('overwrite') \
    .saveAsTable('pargo_gold.parcel_status_daily')

print(f"Gold parcel_status_daily: {parcel_status_daily.count():,} rows")

# COMMAND ----------

# ── 2. RTS Analysis by retailer × province × month ──────────────────────────
rts_analysis = (
    parcels
    .filter(F.col('created_date').isNotNull())
    .withColumn('month', F.date_format('created_date', 'yyyy-MM'))
    .groupBy('month', 'retailer_id', 'province')
    .agg(
        F.count('*').alias('total_parcels'),
        F.sum(F.when(F.col('is_rts') == True, 1).otherwise(0)).alias('rts_parcels'),
        F.sum(F.when(F.col('status') == 'RTS', 1).otherwise(0)).alias('rts_status_count'),
        F.avg('collection_attempts').alias('avg_attempts'),
        F.countDistinct('pickup_point_id').alias('pickup_points_used')
    )
    .withColumn('rts_rate_pct',
        F.round(F.col('rts_parcels') * 100.0 / F.greatest(F.col('total_parcels'), F.lit(1)), 2)
    )
    # Estimated cost impact: R45 per RTS parcel
    .withColumn('estimated_rts_cost_zar',
        F.col('rts_parcels') * 45
    )
    .withColumn('_gold_loaded_at', F.current_timestamp())
    .orderBy('month', F.col('rts_rate_pct').desc())
)

rts_analysis.write.format('delta').mode('overwrite') \
    .saveAsTable('pargo_gold.rts_analysis')

print(f"Gold rts_analysis: {rts_analysis.count():,} rows")

# COMMAND ----------

# ── 3. Hub throughput daily ──────────────────────────────────────────────────
hub_throughput = (
    events
    .filter(F.col('event_date').isNotNull() & F.col('hub_id').isNotNull())
    .groupBy('event_date', 'hub_id', 'event_type')
    .agg(
        F.count('*').alias('scan_count'),
        F.countDistinct('parcel_id').alias('unique_parcels'),
        F.countDistinct('location_city').alias('cities_served')
    )
    .withColumn('_gold_loaded_at', F.current_timestamp())
    .orderBy('event_date', 'hub_id')
)

hub_throughput.write.format('delta').mode('overwrite') \
    .saveAsTable('pargo_gold.hub_throughput_daily')

print(f"Gold hub_throughput_daily: {hub_throughput.count():,} rows")

# COMMAND ----------

# ── 4. Courier performance monthly ──────────────────────────────────────────
courier_perf = (
    parcels
    .filter(F.col('created_date').isNotNull() & (F.col('courier_id') != 'UNASSIGNED'))
    .withColumn('month', F.date_format('created_date', 'yyyy-MM'))
    .groupBy('month', 'courier_id')
    .agg(
        F.count('*').alias('total_assigned'),
        F.sum(F.when(F.col('status') == 'DELIVERED', 1).otherwise(0)).alias('delivered'),
        F.sum(F.when(F.col('status') == 'FAILED', 1).otherwise(0)).alias('failed'),
        F.sum(F.when(F.col('status') == 'RTS', 1).otherwise(0)).alias('returned'),
        F.avg('collection_attempts').alias('avg_attempts')
    )
    .withColumn('delivery_rate_pct',
        F.round(F.col('delivered') * 100.0 / F.greatest(F.col('total_assigned'), F.lit(1)), 2)
    )
    .withColumn('failure_rate_pct',
        F.round(F.col('failed') * 100.0 / F.greatest(F.col('total_assigned'), F.lit(1)), 2)
    )
    .withColumn('_gold_loaded_at', F.current_timestamp())
    .orderBy('month', F.col('delivery_rate_pct').desc())
)

courier_perf.write.format('delta').mode('overwrite') \
    .saveAsTable('pargo_gold.courier_performance')

print(f"Gold courier_performance: {courier_perf.count():,} rows")

# COMMAND ----------

# ── 5. Executive monthly summary ─────────────────────────────────────────────
parcels_with_order = parcels.join(_orders_join, on='parcel_id', how='left')

exec_summary = (
    parcels_with_order
    .filter(F.col('created_date').isNotNull())
    .withColumn('month', F.date_format('created_date', 'yyyy-MM'))
    .groupBy('month')
    .agg(
        F.count('parcel_id').alias('total_parcels'),
        F.countDistinct('parcel_id').alias('unique_parcels'),
        F.sum(F.when(F.col('status') == 'DELIVERED', 1).otherwise(0)).alias('delivered'),
        F.sum(F.when(F.col('status') == 'COLLECTED', 1).otherwise(0)).alias('collected'),
        F.sum(F.when(F.col('status') == 'RTS', 1).otherwise(0)).alias('rts'),
        F.sum(F.when(F.col('status') == 'FAILED', 1).otherwise(0)).alias('failed'),
        F.sum(F.when(F.col('status') == 'PENDING', 1).otherwise(0)).alias('pending'),
        F.sum(F.when(F.col('is_rts') == True, 1).otherwise(0)).alias('rts_flagged'),
        F.countDistinct('retailer_id').alias('active_retailers'),
        F.countDistinct('pickup_point_id').alias('active_pickup_points'),
        F.countDistinct('courier_id').alias('active_couriers'),
        F.avg('weight_kg').alias('avg_weight_kg'),
        F.sum('order_value_zar').alias('total_gmv_zar')
    )
    .withColumn('delivery_success_rate',
        F.round((F.col('delivered') + F.col('collected')) * 100.0 / F.greatest(F.col('unique_parcels'), F.lit(1)), 2)
    )
    .withColumn('rts_rate_pct',
        F.round(F.col('rts') * 100.0 / F.greatest(F.col('unique_parcels'), F.lit(1)), 2)
    )
    # R45 RTS cost estimate
    .withColumn('estimated_rts_cost_zar', F.col('rts_flagged') * 45)
    .withColumn('_gold_loaded_at', F.current_timestamp())
    .orderBy('month')
)

exec_summary.write.format('delta').mode('overwrite') \
    .saveAsTable('pargo_gold.exec_summary_monthly')

print(f"Gold exec_summary_monthly: {exec_summary.count():,} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview executive summary
# MAGIC SELECT month, total_parcels, delivery_success_rate, rts_rate_pct,
# MAGIC        estimated_rts_cost_zar, total_gmv_zar, active_retailers
# MAGIC FROM pargo_gold.exec_summary_monthly
# MAGIC ORDER BY month

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 RTS offenders (retailer × province)
# MAGIC SELECT retailer_id, province, SUM(rts_parcels) AS total_rts,
# MAGIC        ROUND(AVG(rts_rate_pct),1) AS avg_rts_pct,
# MAGIC        SUM(estimated_rts_cost_zar) AS total_rts_cost_zar
# MAGIC FROM pargo_gold.rts_analysis
# MAGIC GROUP BY retailer_id, province
# MAGIC ORDER BY avg_rts_pct DESC
# MAGIC LIMIT 10

# COMMAND ----------

print("✓ Gold layer complete")
print("  pargo_gold.parcel_status_daily   — daily status counts for ops dashboard")
print("  pargo_gold.rts_analysis          — RTS rate by retailer/province/month")
print("  pargo_gold.hub_throughput_daily  — hub scan volumes per day")
print("  pargo_gold.courier_performance   — delivery success rate by courier")
print("  pargo_gold.exec_summary_monthly  — monthly executive KPI summary")
