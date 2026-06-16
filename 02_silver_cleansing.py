# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver Layer — Data Cleansing & Validation
# MAGIC
# MAGIC **Medallion layer:** Silver
# MAGIC **Source:** `pargo_bronze.*_raw` Delta tables
# MAGIC **Output:** `pargo_silver.*` clean, typed, deduplicated Delta tables
# MAGIC
# MAGIC ## Cleaning rules applied
# MAGIC | Issue | Fix |
# MAGIC |-------|-----|
# MAGIC | Duplicate rows (4%) | Deduplicate on business key using `ROW_NUMBER()` |
# MAGIC | Null parcel_id | Drop row — cannot identify the record |
# MAGIC | Mixed-case status | Normalise to canonical uppercase values |
# MAGIC | Invalid status values | Map to `UNKNOWN` |
# MAGIC | Negative weights | Set to NULL |
# MAGIC | Multiple date formats | `try_to_date()` — tolerates bad values, returns NULL |
# MAGIC | Mixed-case boolean flags | Normalise to BOOLEAN |
# MAGIC | Null courier_id | Retain row, flag as UNASSIGNED |
# MAGIC | Mixed currency values | Normalise to ZAR |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# try_to_date helper — tolerates unparseable values (returns NULL instead of throwing)
# Required because Databricks Serverless runs with ANSI mode ON
def parse_date(col_name):
    return F.coalesce(
        F.expr(f"try_to_date(`{col_name}`, 'yyyy-MM-dd')"),
        F.expr(f"try_to_date(`{col_name}`, 'dd/MM/yyyy')"),
        F.expr(f"try_to_date(`{col_name}`, 'dd-MM-yyyy')"),
        F.expr(f"try_to_date(`{col_name}`, 'yyyyMMdd')"),
        F.expr(f"try_to_date(`{col_name}`, 'dd MMM yyyy')")
    )

# COMMAND ----------

# ── 1. PARCELS — deduplicate & clean ────────────────────────────────────────
raw_parcels = spark.table('pargo_bronze.parcels_raw')

# Deduplication: keep latest load_ts per parcel_id
w = Window.partitionBy('parcel_id').orderBy(F.col('load_ts').desc())
deduped = raw_parcels.withColumn('rn', F.row_number().over(w)).filter('rn = 1').drop('rn')

# Status canonical mapping
status_map = {
    'DELIVERED':'DELIVERED','delivered':'DELIVERED','Delivered':'DELIVERED','DELVRD':'DELIVERED',
    'IN_TRANSIT':'IN_TRANSIT','in_transit':'IN_TRANSIT','In Transit':'IN_TRANSIT','IN TRANSIT':'IN_TRANSIT',
    'PENDING':'PENDING','pending':'PENDING','Pending':'PENDING','PNDG':'PENDING',
    'RTS':'RTS','rts':'RTS','Return to Sender':'RTS','RETURN_TO_SENDER':'RTS',
    'FAILED':'FAILED','failed':'FAILED','Failed Delivery':'FAILED','FAIL':'FAILED',
    'COLLECTED':'COLLECTED','collected':'COLLECTED','Collected':'COLLECTED',
}
status_mapping_expr = F.create_map([F.lit(x) for pair in status_map.items() for x in pair])

silver_parcels = (
    deduped
    .filter(F.col('parcel_id').isNotNull() & (F.col('parcel_id') != ''))

    # Normalise status
    .withColumn('status',
        F.coalesce(
            status_mapping_expr[F.col('status')],
            F.lit('UNKNOWN')
        )
    )

    # Parse weight — clamp negatives to NULL
    .withColumn('weight_kg',
        F.when(F.col('weight_kg').isNull(), F.lit(None).cast('double'))
         .otherwise(
            F.when(F.col('weight_kg').cast('double') < 0, F.lit(None))
             .otherwise(F.col('weight_kg').cast('double'))
         )
    )

    # Parse dates using try_to_date (tolerates bad values)
    .withColumn('created_date', parse_date('created_date'))
    .withColumn('updated_date', parse_date('updated_date'))

    # Normalise rts_flag to boolean
    .withColumn('is_rts',
        F.when(F.upper(F.col('rts_flag')).isin('Y','YES','1','TRUE'), F.lit(True))
         .when(F.upper(F.col('rts_flag')).isin('N','NO','0','FALSE'), F.lit(False))
         .otherwise(F.lit(None).cast('boolean'))
    )

    # Normalise fragile flag
    .withColumn('is_fragile',
        F.when(F.upper(F.col('fragile')).isin('TRUE','1','YES'), F.lit(True))
         .when(F.upper(F.col('fragile')).isin('FALSE','0','NO'), F.lit(False))
         .otherwise(F.lit(False))
    )

    # collection_attempts — clamp negatives to 0
    .withColumn('collection_attempts',
        F.greatest(F.lit(0), F.col('collection_attempts').cast('int'))
    )

    # Province — title case, nullify blanks
    .withColumn('province',
        F.when(F.col('province').isNull() | (F.trim(F.col('province')) == ''), F.lit(None))
         .otherwise(F.initcap(F.trim(F.col('province'))))
    )

    # Courier — trim, upper, unify separators; null/blank → UNASSIGNED
    .withColumn('courier_id',
        F.when(F.col('courier_id').isNull() | (F.trim(F.col('courier_id')) == ''), F.lit('UNASSIGNED'))
         .otherwise(F.upper(F.regexp_replace(F.trim(F.col('courier_id')), r'[\s\-]', '_')))
    )

    # Data quality flags
    .withColumn('dq_weight_null',   F.col('weight_kg').isNull())
    .withColumn('dq_date_unparsed', F.col('created_date').isNull())
    .withColumn('dq_province_null', F.col('province').isNull())

    .withColumn('_silver_loaded_at', F.current_timestamp())
    .drop('rts_flag', 'fragile', 'load_ts')
)

silver_parcels.write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable('pargo_silver.parcels')

print(f"Silver parcels: {silver_parcels.count():,} rows written")

# COMMAND ----------

# ── 2. TRACKING EVENTS — deduplicate & clean ────────────────────────────────
raw_events = spark.table('pargo_bronze.tracking_events_raw')

event_type_map = {
    'SCAN_IN':'SCAN_IN','scan_in':'SCAN_IN','Scan In':'SCAN_IN',
    'SCAN_OUT':'SCAN_OUT','scan_out':'SCAN_OUT',
    'DELIVERED':'DELIVERED','delivered':'DELIVERED',
    'COLLECTED':'COLLECTED','collected':'COLLECTED',
    'OUT_FOR_DELIVERY':'OUT_FOR_DELIVERY','out for delivery':'OUT_FOR_DELIVERY',
    'ATTEMPTED':'ATTEMPTED','attempted':'ATTEMPTED',
    'RETURNED':'RETURNED','returned':'RETURNED',
    'RTS':'RTS',
}
et_map_expr = F.create_map([F.lit(x) for pair in event_type_map.items() for x in pair])

w_evt = Window.partitionBy('event_id').orderBy(F.col('load_ts').desc())

silver_events = (
    raw_events
    .withColumn('rn', F.row_number().over(w_evt)).filter('rn = 1').drop('rn')
    .filter(F.col('event_id').isNotNull() & F.col('parcel_id').isNotNull())
    .withColumn('event_type',
        F.coalesce(et_map_expr[F.col('event_type')], F.lit('UNKNOWN'))
    )
    .withColumn('event_date', parse_date('event_date'))
    .withColumn('latitude',
        F.when(
            (F.col('latitude').cast('double') < -90) | (F.col('latitude').cast('double') > 90),
            F.lit(None)
        ).otherwise(F.col('latitude').cast('double'))
    )
    .withColumn('longitude',
        F.when(
            (F.col('longitude').cast('double') < -180) | (F.col('longitude').cast('double') > 180),
            F.lit(None)
        ).otherwise(F.col('longitude').cast('double'))
    )
    .withColumn('location_city',
        F.when(F.col('location_city').isNull() | (F.trim(F.col('location_city')) == ''), F.lit(None))
         .otherwise(F.initcap(F.trim(F.col('location_city'))))
    )
    .withColumn('_silver_loaded_at', F.current_timestamp())
    .drop('load_ts')
)

silver_events.write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable('pargo_silver.tracking_events')

print(f"Silver events: {silver_events.count():,} rows written")

# COMMAND ----------

# ── 3. ORDERS — deduplicate & clean ─────────────────────────────────────────
raw_orders = spark.table('pargo_bronze.orders_raw')

w_ord = Window.partitionBy('order_id', 'parcel_id').orderBy(F.col('load_ts').desc())

silver_orders = (
    raw_orders
    .withColumn('rn', F.row_number().over(w_ord)).filter('rn = 1').drop('rn')
    .filter(F.col('parcel_id').isNotNull())
    .withColumn('order_date', parse_date('order_date'))
    .withColumn('order_value_zar',
        F.when(F.col('order_value').cast('double') < 0, F.lit(None))
         .otherwise(F.col('order_value').cast('double'))
    )
    .withColumn('currency',
        F.when(F.upper(F.trim(F.col('currency'))).isin('ZAR', 'R'), F.lit('ZAR'))
         .when(F.col('currency').isNull() | (F.trim(F.col('currency')) == ''), F.lit('ZAR'))
         .otherwise(F.upper(F.trim(F.col('currency'))))
    )
    .withColumn('payment_method',
        F.upper(F.coalesce(F.trim(F.col('payment_method')), F.lit('UNKNOWN')))
    )
    .withColumn('_silver_loaded_at', F.current_timestamp())
    .drop('order_value', 'load_ts')
)

silver_orders.write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable('pargo_silver.orders')

print(f"Silver orders: {silver_orders.count():,} rows written")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'parcels' AS tbl,
# MAGIC   COUNT(*)                                                          AS rows,
# MAGIC   ROUND(SUM(CASE WHEN dq_weight_null   THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS pct_null_weight,
# MAGIC   ROUND(SUM(CASE WHEN dq_date_unparsed THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS pct_unparsed_date,
# MAGIC   ROUND(SUM(CASE WHEN dq_province_null THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS pct_null_province,
# MAGIC   COUNT(DISTINCT status)                                            AS distinct_statuses
# MAGIC FROM pargo_silver.parcels

# COMMAND ----------

print("Silver layer complete")
print("  pargo_silver.parcels          -- deduped, typed, status normalised, booleans clean")
print("  pargo_silver.tracking_events  -- deduped, event_type normalised, lat/lon validated")
print("  pargo_silver.orders           -- deduped, order_value_zar, currency normalised")
print("Next: Run 03_gold_analytics")
