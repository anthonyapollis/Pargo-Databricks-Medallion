# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Machine Learning — RTS Prediction Model
# MAGIC
# MAGIC **Goal:** Predict whether a parcel will be returned to sender (RTS) before it happens.
# MAGIC Early identification = lower re-delivery cost (R45/parcel saving).
# MAGIC
# MAGIC **Pipeline:**
# MAGIC 1. Pull features from Silver + Gold Delta tables
# MAGIC 2. Engineer features (encode categoricals, derive ratios)
# MAGIC 3. Train XGBoost classifier with MLflow auto-logging
# MAGIC 4. Evaluate (ROC-AUC, precision, recall, confusion matrix)
# MAGIC 5. Register best model to Databricks Model Registry

# COMMAND ----------

# MAGIC %pip install xgboost scikit-learn matplotlib seaborn --quiet

# COMMAND ----------

import mlflow
import mlflow.xgboost
import mlflow.sklearn
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (roc_auc_score, classification_report,
                             confusion_matrix, RocCurveDisplay)
from sklearn.utils.class_weight import compute_sample_weight
from pyspark.sql import functions as F

mlflow.set_experiment("/Pargo_Medallion/RTS_Prediction")
print("MLflow experiment set: /Pargo_Medallion/RTS_Prediction")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Feature Engineering

# COMMAND ----------

# Load Silver parcels
parcels_df = spark.table('pargo_silver.parcels').select(
    'parcel_id', 'retailer_id', 'pickup_point_id', 'courier_id',
    'status', 'is_rts', 'weight_kg', 'is_fragile',
    'collection_attempts', 'province', 'created_date'
)

# Load Gold courier performance for derived features
courier_perf = spark.table('pargo_gold.courier_performance').select(
    'courier_id', 'month',
    'delivery_rate_pct', 'failure_rate_pct', F.col('avg_attempts').alias('courier_avg_attempts')
)

# Load Gold RTS analysis for retailer-level RTS rate
retailer_rts = spark.table('pargo_gold.rts_analysis').select(
    'retailer_id', 'month', 'rts_rate_pct', 'avg_attempts'
).withColumnRenamed('avg_attempts', 'retailer_avg_attempts')

# Add month key to parcels for joining
parcels_df = parcels_df.withColumn('month', F.date_format('created_date', 'yyyy-MM'))

# Join courier performance
parcels_df = parcels_df.join(courier_perf, on=['courier_id', 'month'], how='left')

# Join retailer RTS rate
parcels_df = parcels_df.join(retailer_rts, on=['retailer_id', 'month'], how='left')

# Convert to pandas for sklearn
pdf = parcels_df.toPandas()
print(f"Dataset shape: {pdf.shape}")
print(f"RTS rate in dataset: {pdf['is_rts'].mean():.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Prepare Features & Target

# COMMAND ----------

# Target: is_rts — True = RTS, False = not RTS
# For rows where is_rts is null, infer from status
pdf['target'] = (
    pdf['is_rts'].fillna(False) |
    (pdf['status'] == 'RTS')
).astype(int)

print(f"Class distribution:\n{pdf['target'].value_counts()}")
print(f"RTS positive rate: {pdf['target'].mean():.1%}")

# COMMAND ----------

# Feature engineering
pdf['weight_kg']           = pd.to_numeric(pdf['weight_kg'], errors='coerce').fillna(pdf['weight_kg'].median())
pdf['collection_attempts'] = pd.to_numeric(pdf['collection_attempts'], errors='coerce').fillna(0).clip(lower=0)
pdf['is_fragile']          = pdf['is_fragile'].fillna(False).astype(int)
pdf['delivery_rate_pct']   = pd.to_numeric(pdf['delivery_rate_pct'], errors='coerce').fillna(pdf['delivery_rate_pct'].median())
pdf['failure_rate_pct']    = pd.to_numeric(pdf['failure_rate_pct'], errors='coerce').fillna(0)
pdf['rts_rate_pct']        = pd.to_numeric(pdf['rts_rate_pct'], errors='coerce').fillna(pdf['rts_rate_pct'].median())
pdf['retailer_avg_attempts'] = pd.to_numeric(pdf['retailer_avg_attempts'], errors='coerce').fillna(1)
pdf['courier_avg_attempts']  = pd.to_numeric(pdf['courier_avg_attempts'], errors='coerce').fillna(1)

# Derived features
pdf['high_attempt_flag']    = (pdf['collection_attempts'] >= 2).astype(int)
pdf['heavy_parcel_flag']    = (pdf['weight_kg'] > 10).astype(int)
pdf['retailer_rts_risk']    = pd.cut(pdf['rts_rate_pct'], bins=[0,5,15,100], labels=[0,1,2]).astype(float).fillna(0)
pdf['courier_poor_flag']    = (pdf['delivery_rate_pct'] < 60).astype(int)

# Encode categoricals
le = LabelEncoder()
for col in ['courier_id', 'province', 'retailer_id']:
    pdf[col + '_enc'] = le.fit_transform(pdf[col].fillna('UNKNOWN').astype(str))

# Final feature set
FEATURES = [
    'weight_kg', 'is_fragile', 'collection_attempts',
    'high_attempt_flag', 'heavy_parcel_flag',
    'delivery_rate_pct', 'failure_rate_pct', 'courier_avg_attempts', 'courier_poor_flag',
    'rts_rate_pct', 'retailer_avg_attempts', 'retailer_rts_risk',
    'courier_id_enc', 'province_enc', 'retailer_id_enc'
]

X = pdf[FEATURES].fillna(0)
y = pdf['target']

print(f"\nFeatures: {FEATURES}")
print(f"X shape: {X.shape}, y shape: {y.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Train / Test Split

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Handle class imbalance with sample weights
sample_weights = compute_sample_weight('balanced', y_train)

print(f"Train: {X_train.shape[0]:,}  |  Test: {X_test.shape[0]:,}")
print(f"Train RTS rate: {y_train.mean():.1%}  |  Test RTS rate: {y_test.mean():.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Train XGBoost with MLflow Tracking

# COMMAND ----------

mlflow.xgboost.autolog(log_models=True)

with mlflow.start_run(run_name="xgboost_rts_v1") as run:

    model = XGBClassifier(
        n_estimators     = 300,
        max_depth        = 5,
        learning_rate    = 0.05,
        subsample        = 0.8,
        colsample_bytree = 0.8,
        min_child_weight = 5,
        scale_pos_weight = (y_train == 0).sum() / (y_train == 1).sum(),
        eval_metric      = 'auc',
        early_stopping_rounds = 20,
        random_state     = 42,
        verbosity        = 0
    )

    model.fit(
        X_train, y_train,
        sample_weight   = sample_weights,
        eval_set        = [(X_test, y_test)],
        verbose         = False
    )

    # ── Evaluation ──────────────────────────────────────────────────────────
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    y_pred       = (y_pred_proba >= 0.4).astype(int)   # lower threshold = catch more RTS

    auc  = roc_auc_score(y_test, y_pred_proba)
    report = classification_report(y_test, y_pred, output_dict=True)

    mlflow.log_metric("roc_auc",   auc)
    mlflow.log_metric("precision", report['1']['precision'])
    mlflow.log_metric("recall",    report['1']['recall'])
    mlflow.log_metric("f1",        report['1']['f1-score'])
    mlflow.log_param("threshold",  0.4)
    mlflow.log_param("features",   str(FEATURES))

    print(f"\nROC-AUC : {auc:.4f}")
    print(f"\nClassification Report:\n{classification_report(y_test, y_pred)}")

    # ── Feature importance plot ───────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(8, 5))
    importances = pd.Series(model.feature_importances_, index=FEATURES).sort_values()
    importances.plot(kind='barh', ax=ax, color='#3B82F6')
    ax.set_title('Feature Importance — RTS Predictor')
    ax.set_xlabel('Gain')
    plt.tight_layout()
    mlflow.log_figure(fig, "feature_importance.png")
    display(fig)
    plt.close()

    # ── Confusion matrix ─────────────────────────────────────────────────
    fig2, ax2 = plt.subplots(figsize=(4, 3))
    cm = confusion_matrix(y_test, y_pred)
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=ax2,
                xticklabels=['Not RTS','RTS'], yticklabels=['Not RTS','RTS'])
    ax2.set_title('Confusion Matrix')
    ax2.set_xlabel('Predicted')
    ax2.set_ylabel('Actual')
    plt.tight_layout()
    mlflow.log_figure(fig2, "confusion_matrix.png")
    display(fig2)
    plt.close()

    # ── ROC curve ────────────────────────────────────────────────────────
    fig3, ax3 = plt.subplots(figsize=(5, 4))
    RocCurveDisplay.from_predictions(y_test, y_pred_proba, ax=ax3, color='#3B82F6')
    ax3.set_title(f'ROC Curve — AUC = {auc:.3f}')
    plt.tight_layout()
    mlflow.log_figure(fig3, "roc_curve.png")
    display(fig3)
    plt.close()

    run_id = run.info.run_id
    print(f"\nMLflow run_id: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Register Model to Databricks Model Registry

# COMMAND ----------

model_uri  = f"runs:/{run_id}/model"
model_name = "pargo_rts_predictor"

reg = mlflow.register_model(model_uri=model_uri, name=model_name)
print(f"Model registered: {model_name}  version={reg.version}")

# Transition to Staging
client = mlflow.tracking.MlflowClient()
client.transition_model_version_stage(
    name    = model_name,
    version = reg.version,
    stage   = "Staging"
)
print(f"Model transitioned to Staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Score Silver Parcels — Attach Predictions Back

# COMMAND ----------

# Load full Silver parcels for scoring
score_pdf = parcels_df.toPandas()

# Apply same feature engineering
score_pdf['weight_kg']           = pd.to_numeric(score_pdf['weight_kg'], errors='coerce').fillna(score_pdf['weight_kg'].median())
score_pdf['collection_attempts'] = pd.to_numeric(score_pdf['collection_attempts'], errors='coerce').fillna(0).clip(lower=0)
score_pdf['is_fragile']          = score_pdf['is_fragile'].fillna(False).astype(int)
score_pdf['delivery_rate_pct']   = pd.to_numeric(score_pdf['delivery_rate_pct'], errors='coerce').fillna(75)
score_pdf['failure_rate_pct']    = pd.to_numeric(score_pdf['failure_rate_pct'], errors='coerce').fillna(0)
score_pdf['rts_rate_pct']        = pd.to_numeric(score_pdf['rts_rate_pct'], errors='coerce').fillna(5)
score_pdf['retailer_avg_attempts'] = pd.to_numeric(score_pdf['retailer_avg_attempts'], errors='coerce').fillna(1)
score_pdf['courier_avg_attempts']  = pd.to_numeric(score_pdf['courier_avg_attempts'], errors='coerce').fillna(1)
score_pdf['high_attempt_flag']   = (score_pdf['collection_attempts'] >= 2).astype(int)
score_pdf['heavy_parcel_flag']   = (score_pdf['weight_kg'] > 10).astype(int)
score_pdf['retailer_rts_risk']   = pd.cut(score_pdf['rts_rate_pct'], bins=[0,5,15,100], labels=[0,1,2]).astype(float).fillna(0)
score_pdf['courier_poor_flag']   = (score_pdf['delivery_rate_pct'] < 60).astype(int)

for col in ['courier_id', 'province', 'retailer_id']:
    score_pdf[col + '_enc'] = le.fit_transform(score_pdf[col].fillna('UNKNOWN').astype(str))

X_score = score_pdf[FEATURES].fillna(0)
score_pdf['rts_probability'] = model.predict_proba(X_score)[:, 1]
score_pdf['rts_predicted']   = (score_pdf['rts_probability'] >= 0.4).astype(int)

# Write predictions to Gold table
predictions_spark = spark.createDataFrame(score_pdf[[
    'parcel_id', 'retailer_id', 'courier_id', 'province',
    'rts_probability', 'rts_predicted', 'status', 'collection_attempts'
]])

predictions_spark.write.format('delta').mode('overwrite') \
    .saveAsTable('pargo_gold.rts_predictions')

flagged = score_pdf['rts_predicted'].sum()
print(f"Parcels scored: {len(score_pdf):,}")
print(f"Predicted RTS:  {flagged:,}  ({flagged/len(score_pdf):.1%})")
print(f"Est. preventable cost: R{flagged * 45:,.0f}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top high-risk parcels
# MAGIC SELECT parcel_id, retailer_id, courier_id, province,
# MAGIC        ROUND(rts_probability * 100, 1) AS rts_prob_pct,
# MAGIC        collection_attempts, status
# MAGIC FROM pargo_gold.rts_predictions
# MAGIC WHERE rts_predicted = 1
# MAGIC ORDER BY rts_probability DESC
# MAGIC LIMIT 20

# COMMAND ----------

print("ML pipeline complete")
print("  Model:      pargo_rts_predictor  (Staging in Model Registry)")
print("  Gold table: pargo_gold.rts_predictions  -- all parcels scored")
print("  MLflow:     Experiments > /Pargo_Medallion/RTS_Prediction")
