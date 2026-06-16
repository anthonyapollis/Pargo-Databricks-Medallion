# Pargo Parcels — Databricks Medallion Analytics Platform

End-to-end serverless data engineering pipeline built on **Databricks Free Edition (AWS)**.
Ingests dirty parcel data, cleanses it through a medallion architecture, builds business KPIs in Gold, and trains an XGBoost RTS prediction model with MLflow tracking.

---

## Architecture

```
Bronze (raw)  →  Silver (clean)  →  Gold (aggregated)  →  ML (XGBoost + MLflow)
pargo_bronze.*    pargo_silver.*      pargo_gold.*          pargo_rts_predictor
```

| Layer  | Tables | Key action |
|--------|--------|------------|
| Bronze | 3      | Permissive STRING ingestion — 52K rows, all dirty data preserved |
| Silver | 3      | Deduplicate, type-cast, normalise statuses, ANSI-safe date parsing |
| Gold   | 5      | Business aggregations — RTS cost, courier performance, exec KPIs |
| ML     | 1      | XGBoost classifier (AUC 0.81) — scores all parcels for RTS risk |

---

## Key Results

| Metric | Value |
|--------|-------|
| Bronze rows ingested | 52,000 |
| Duplicates removed (Silver) | 2,001 |
| Silver clean records | 49,999 |
| Canonical statuses | 7 |
| Gold tables | 5 |
| ML ROC-AUC | **0.81** |
| Precision / Recall | 0.74 / 0.68 |
| Est. RTS cost saving (30% reduction) | **R502,200 / year** |

---

## Files

| File | Description |
|------|-------------|
| `01_bronze_ingestion.py` | Generates 52K synthetic dirty Pargo parcels as Delta tables |
| `02_silver_cleansing.py` | Dedup, normalise, type-cast — ANSI-safe `try_to_date()` |
| `03_gold_analytics.py` | 5 Gold aggregation tables — RTS cost R45/parcel |
| `04_ml_rts_prediction.py` | XGBoost + MLflow + Model Registry |
| `upload_to_databricks.py` | REST API deployment — uploads notebooks + creates Workflow |
| `pargo_databricks_dashboard.html` | **Interactive hiring manager dashboard** — open in browser |
| `pargo_databricks_ebook.html` | **Full technical ebook** — printable to PDF |
| `Pargo_Databricks_Medallion_Project.xlsx` | **Excel workbook** — 7 sheets with data, charts, model card |
| `generate_excel.py` | Generates the Excel workbook locally (requires `openpyxl`) |

---

## Technical Highlights

- **ANSI mode fix** — Databricks Serverless enforces ANSI SQL; `to_date()` throws on bad values. Fixed with `try_to_date()` via `F.expr()` in a reusable `parse_date()` helper.
- **Serverless-only Workflow** — Databricks Free Edition disallows `job_clusters` in job definitions. Deployed 4-task DAG without any cluster config — serverless is implicit.
- **Ambiguous join resolved** — pre-dropped `retailer_id` / `customer_id` from orders before joining with parcels to avoid `AMBIGUOUS_REFERENCE` in Spark SQL.
- **Class imbalance** — handled with `scale_pos_weight` (~14×) + `compute_sample_weight('balanced')` + threshold lowered to 0.4 for higher RTS recall.
- **API-first deployment** — all notebooks deployed via Databricks REST API (`/api/2.0/workspace/import`) without touching the UI.

---

## Running locally

```bash
# Install dependencies
pip install openpyxl

# Generate the Excel workbook
python generate_excel.py

# Deploy notebooks to your Databricks workspace
python upload_to_databricks.py <YOUR_PAT_TOKEN>
```

Open `pargo_databricks_dashboard.html` in any browser for the interactive portfolio showcase.

---

## Tech Stack

`Apache Spark (PySpark)` · `Delta Lake` · `MLflow` · `XGBoost` · `Databricks Serverless`
`Databricks Workflows` · `scikit-learn` · `pandas` · `matplotlib` · `Python 3.11`

---

**Author:** Anthony Apollis · Data Engineer & Analytics Specialist · Cape Town  
**Contact:** anthony.apollis@gmail.com · [anthonyapollis.github.io](https://anthonyapollis.github.io)
