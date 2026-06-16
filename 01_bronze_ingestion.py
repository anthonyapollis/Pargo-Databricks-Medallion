# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze Layer — Pargo Raw Data Ingestion
# MAGIC
# MAGIC **Medallion layer:** Bronze
# MAGIC **Purpose:** Generate realistic dirty Pargo parcel data and land it as-is into Delta tables.
# MAGIC Dirty issues injected: nulls (8–15%), mixed-case statuses, duplicate rows (4%), negative weights, mixed date formats, invalid courier IDs.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS pargo_bronze COMMENT 'Raw Pargo data — no cleansing applied';
# MAGIC CREATE DATABASE IF NOT EXISTS pargo_silver COMMENT 'Cleaned and validated Pargo data';
# MAGIC CREATE DATABASE IF NOT EXISTS pargo_gold   COMMENT 'Business-ready Pargo aggregations';

# COMMAND ----------

import random
import string
from datetime import datetime, timedelta
from pyspark.sql import Row
from pyspark.sql.types import *

random.seed(42)

# ── Reference data ──────────────────────────────────────────────────────────
STATUSES_DIRTY = [
    'DELIVERED','delivered','Delivered','DELVRD',
    'IN_TRANSIT','in_transit','In Transit','IN TRANSIT',
    'PENDING','pending','Pending','PNDG',
    'RTS','rts','Return to Sender','RETURN_TO_SENDER',
    'FAILED','failed','Failed Delivery','FAIL',
    'COLLECTED','collected','Collected',
    None, '', 'UNKNOWN', 'N/A'
]
DATE_FORMATS = ['%Y-%m-%d','%d/%m/%Y','%d-%m-%Y','%Y%m%d','%d %b %Y']
COURIERS = ['CourierA','COURIER_A','courier a','CourierB','COURIER-B','couriercb',
            'FastShip','fast ship','FASTSHIP','QuickDel','QUICK_DEL',None,'']
PICKUP_POINT_IDS = [f'PP{str(i).zfill(4)}' for i in range(1, 201)]
RETAILER_IDS     = [f'RET{str(i).zfill(3)}' for i in range(1, 51)]
CUSTOMER_IDS     = [f'CUST{str(i).zfill(5)}' for i in range(1, 5001)]

def rand_date(start='2023-07-01', end='2026-06-01', fmt=None):
    s = datetime.strptime(start, '%Y-%m-%d')
    e = datetime.strptime(end,   '%Y-%m-%d')
    d = s + timedelta(days=random.randint(0, (e - s).days))
    return d.strftime(fmt or random.choice(DATE_FORMATS))

def maybe_null(v, p=0.08):
    return None if random.random() < p else v

def rand_id(prefix, n=8):
    return prefix + ''.join(random.choices(string.digits, k=n))

# ── Generate dirty parcels ───────────────────────────────────────────────────
NUM_PARCELS = 50000
parcel_ids  = [rand_id('PRC', 10) for _ in range(NUM_PARCELS)]

def make_parcel(pid):
    weight = round(random.uniform(-0.5 if random.random() < 0.03 else 0.1, 25.0), 2)
    return Row(
        parcel_id        = pid,
        order_id         = maybe_null(rand_id('ORD', 9), 0.02),
        pickup_point_id  = maybe_null(random.choice(PICKUP_POINT_IDS), 0.05),
        retailer_id      = maybe_null(random.choice(RETAILER_IDS), 0.04),
        customer_id      = maybe_null(random.choice(CUSTOMER_IDS), 0.03),
        courier_id       = maybe_null(random.choice(COURIERS), 0.10),
        status           = maybe_null(random.choice(STATUSES_DIRTY), 0.06),
        created_date     = maybe_null(rand_date(), 0.04),
        updated_date     = maybe_null(rand_date(), 0.08),
        weight_kg        = maybe_null(str(weight), 0.09),
        fragile          = maybe_null(random.choice(['true','True','TRUE','1','false','False','0',None]), 0.07),
        rts_flag         = maybe_null(random.choice(['Y','N','YES','NO','1','0',None,'y','n']), 0.05),
        collection_attempts = maybe_null(str(random.randint(-1 if random.random()<0.02 else 0, 5)), 0.06),
        province         = maybe_null(random.choice(['Western Cape','western cape','WC','Gauteng','GP','KZN','KwaZulu-Natal',None,'']), 0.12),
        load_ts          = datetime.now().isoformat()
    )

parcels = [make_parcel(pid) for pid in parcel_ids]

# Inject 4% duplicate rows
dup_parcels = random.sample(parcels, int(NUM_PARCELS * 0.04))
parcels += dup_parcels
random.shuffle(parcels)

df_parcels = spark.createDataFrame(parcels)
df_parcels.write.format('delta').mode('overwrite').saveAsTable('pargo_bronze.parcels_raw')
print(f"Bronze parcels written: {len(parcels):,} rows ({int(NUM_PARCELS*0.04):,} intentional duplicates)")

# COMMAND ----------

# ── Generate dirty tracking events ──────────────────────────────────────────
EVENT_TYPES = ['SCAN_IN','scan_in','Scan In','SCAN_OUT','scan_out','DELIVERED','delivered',
               'COLLECTED','OUT_FOR_DELIVERY','out for delivery','ATTEMPTED','attempted',
               'RETURNED','returned','RTS',None,'UNKNOWN']
HUB_IDS = [f'HUB{str(i).zfill(3)}' for i in range(1, 31)]

def make_event(pid):
    return Row(
        event_id       = rand_id('EVT', 12),
        parcel_id      = pid,
        event_type     = maybe_null(random.choice(EVENT_TYPES), 0.06),
        event_date     = maybe_null(rand_date(), 0.08),
        hub_id         = maybe_null(random.choice(HUB_IDS), 0.10),
        location_city  = maybe_null(random.choice(['Cape Town','cape town','CPT','Johannesburg','JHB','Durban','DUR',None,'']), 0.13),
        latitude       = maybe_null(str(round(random.uniform(-35, -22), 6)), 0.15),
        longitude      = maybe_null(str(round(random.uniform(16, 33), 6)), 0.15),
        operator_id    = maybe_null(rand_id('OPR', 4), 0.05),
        load_ts        = datetime.now().isoformat()
    )

# ~3 events per parcel on average
event_rows = [make_event(pid) for pid in parcel_ids for _ in range(random.randint(1, 6))]
dup_events = random.sample(event_rows, int(len(event_rows) * 0.03))
event_rows += dup_events

df_events = spark.createDataFrame(event_rows)
df_events.write.format('delta').mode('overwrite').saveAsTable('pargo_bronze.tracking_events_raw')
print(f"Bronze tracking events written: {len(event_rows):,} rows")

# COMMAND ----------

# ── Generate dirty orders ────────────────────────────────────────────────────
def make_order(pid):
    value = round(random.uniform(-50 if random.random() < 0.02 else 0, 5000), 2)
    return Row(
        order_id       = maybe_null(rand_id('ORD', 9), 0.02),
        parcel_id      = pid,
        retailer_id    = maybe_null(random.choice(RETAILER_IDS), 0.04),
        customer_id    = maybe_null(random.choice(CUSTOMER_IDS), 0.03),
        order_date     = maybe_null(rand_date(), 0.06),
        order_value    = maybe_null(str(value), 0.10),
        currency       = maybe_null(random.choice(['ZAR','zar','Zar','R','USD',None,'']), 0.05),
        payment_method = maybe_null(random.choice(['CARD','card','EFT','CASH','cash',None]), 0.08),
        load_ts        = datetime.now().isoformat()
    )

order_rows = [make_order(pid) for pid in parcel_ids]
dup_orders = random.sample(order_rows, int(len(order_rows) * 0.03))
order_rows += dup_orders

df_orders = spark.createDataFrame(order_rows)
df_orders.write.format('delta').mode('overwrite').saveAsTable('pargo_bronze.orders_raw')
print(f"Bronze orders written: {len(order_rows):,} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick profile of dirty bronze data
# MAGIC SELECT
# MAGIC   COUNT(*)                                            AS total_rows,
# MAGIC   COUNT(DISTINCT parcel_id)                          AS unique_parcel_ids,
# MAGIC   SUM(CASE WHEN status IS NULL OR status = '' THEN 1 ELSE 0 END) AS null_status,
# MAGIC   SUM(CASE WHEN pickup_point_id IS NULL THEN 1 ELSE 0 END)       AS null_pickup_point,
# MAGIC   SUM(CASE WHEN CAST(weight_kg AS DOUBLE) < 0 THEN 1 ELSE 0 END) AS negative_weights,
# MAGIC   COUNT(*) - COUNT(DISTINCT parcel_id)               AS duplicate_count
# MAGIC FROM pargo_bronze.parcels_raw

# COMMAND ----------

print("✓ Bronze layer complete — 3 dirty Delta tables created in pargo_bronze database")
print("  pargo_bronze.parcels_raw       — raw parcel records with dirty data")
print("  pargo_bronze.tracking_events_raw — raw tracking scan events")
print("  pargo_bronze.orders_raw        — raw order records")
print("\nNext: Run 02_silver_cleansing to clean and validate the data")
