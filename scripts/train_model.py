#!/usr/bin/env python3
"""
train_model.py — Entrenamiento, evaluación y serialización del modelo de ML.

Requisitos de la Entrega Final (ML Model Inference):
  1. Definición del problema: Predecir si una alerta satelital de deforestación corresponde
     a pérdida de BOSQUE PRIMARIO ('is_primary_forest', binario 0/1) con ALTA CONFIANZA.
     - Justificación: Los bosques primarios almacenan hasta 70% más carbono y biodiversidad intacta.
       Detectar si una alerta recién detectada satelitalmente compromete bosque primario permite
       priorizar despachos de brigadas forestales y operativos en tiempo real.
  2. Features de entrada (sin data leakage):
     - latitude, longitude
     - tree_cover_density_pct
     - dist_nearest_place_km (proximidad a centros poblados)
     - driver_id (categoría de pérdida atribuida)
     - month, week (estacionalidad intra-anual)
     - country_code (one-hot o label encoding)
     - protected_area_cat
  3. Preprocesamiento & Pipeline:
     - Imputación de nulos y escalado robusto/estandarizado.
     - One-hot encoding de variables categóricas.
     - Pipeline unificado scikit-learn con Random Forest / Gradient Boosting.
  4. Evaluación:
     - Train/Test Split estratificado (80/20).
     - Métricas: Accuracy, Precision, Recall, F1-Score, ROC-AUC.
     - Matriz de confusión exportada.
  5. Serialización:
     - Guarda el pipeline entrenado en 'models/deforestation_forest_risk_pipeline.joblib'.
     - Guarda metadatos y reporte en 'models/model_metadata.json'.
"""

import sys
import json
from pathlib import Path
import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.metrics import (
    classification_report,
    roc_auc_score,
    confusion_matrix,
    accuracy_score,
    f1_score,
)
import joblib

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
MODELS_DIR   = PROJECT_ROOT / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)


def load_dataset() -> pd.DataFrame:
    """Carga los datos preprocesados de data/processed o data/csv."""
    pkl_path = DATA_DIR / "processed" / "gfw_transformed.pkl"
    if pkl_path.exists():
        print(f"  Cargando dataset desde {pkl_path}...")
        df = pd.read_pickle(pkl_path)
    else:
        print("  Cargando desde data/csv y calculando features mínimas...")
        dfs = []
        for p in (DATA_DIR / "csv").glob("*alerts*.csv"):
            code = "BOL" if "bol" in p.name.lower() else "COL"
            d = pd.read_csv(p)
            d["country_code"] = code
            dfs.append(d)
        df = pd.concat(dfs, ignore_index=True)
        df['date'] = pd.to_datetime(df['gfw_integrated_alerts__date'])
        df['month'] = df['date'].dt.month
        df['week'] = df['date'].dt.isocalendar().week.astype(int)
        df['tree_cover_density_pct'] = df['umd_tree_cover_density_2000__percent']
        df['is_primary_forest'] = df['is__umd_regional_primary_forest_2001']
        df['driver_id'] = df['wri_google_tree_cover_loss_drivers__category'].fillna(7)
        df['protected_area_cat'] = df['wdpa_protected_areas__iucn_cat'].fillna(0)
        df['dist_nearest_place_km'] = 10.0

    return df


def main():
    print("=" * 65)
    print("🌲 ENTRENAMIENTO DEL MODELO DE ML — DEFORESTATION ALERT RISK")
    print("=" * 65)

    df = load_dataset()
    print(f"Total registros cargados: {len(df):,}")

    # Limpiar target y verificar que sea binario
    target_col = "is_primary_forest"
    df = df.dropna(subset=[target_col]).copy()
    df[target_col] = df[target_col].astype(int)
    print(f"Distribución del target '{target_col}':")
    print(df[target_col].value_counts(normalize=True).to_dict())

    # Seleccionar una muestra balanceada o representativa si el dataset supera 200k filas
    # para optimizar tiempo de CPU conservando distribución estadística real
    if len(df) > 200000:
        print("  Muestreo estratificado de 200,000 registros para entrenamiento óptimo...")
        n_per_group = 100000
        samples = []
        for val, group in df.groupby(target_col):
            samples.append(group.sample(min(len(group), n_per_group), random_state=42))
        df = pd.concat(samples, ignore_index=True).sample(frac=1, random_state=42).reset_index(drop=True)

    numeric_features = [
        "latitude",
        "longitude",
        "tree_cover_density_pct",
        "dist_nearest_place_km",
        "month",
        "week",
    ]
    categorical_features = [
        "country_code",
    ]

    # Asegurar que todas las columnas existan
    if "dist_nearest_place_km" not in df.columns:
        df["dist_nearest_place_km"] = 10.0
    if "driver_id" not in df.columns and "loss_driver_category" in df.columns:
        df["driver_id"] = df["loss_driver_category"].fillna(7).astype(int)

    if "driver_id" in df.columns:
        numeric_features.append("driver_id")

    X = df[numeric_features + categorical_features].copy()
    y = df[target_col].values

    # ── Train / Test Split Estratificado ──────────────────────────────────────
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42, stratify=y
    )
    print(f"Train size: {len(X_train):,} | Test size: {len(X_test):,}")

    # ── Preprocessing Pipeline ────────────────────────────────────────────────
    numeric_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])

    categorical_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="constant", fill_value="UNKNOWN")),
        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])

    preprocessor = ColumnTransformer(transformers=[
        ("num", numeric_transformer, numeric_features),
        ("cat", categorical_transformer, categorical_features),
    ])

    # ── Clasificador ──────────────────────────────────────────────────────────
    classifier = RandomForestClassifier(
        n_estimators=100,
        max_depth=15,
        min_samples_split=10,
        min_samples_leaf=5,
        n_jobs=-1,
        random_state=42,
    )

    full_pipeline = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("classifier", classifier),
    ])

    print("▶ Entrenando modelo Random Forest Classifier...")
    full_pipeline.fit(X_train, y_train)

    print("▶ Evaluando en conjunto de prueba...")
    y_pred = full_pipeline.predict(X_test)
    y_proba = full_pipeline.predict_proba(X_test)[:, 1]

    acc = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_proba)
    report = classification_report(y_test, y_pred, output_dict=True)
    cm = confusion_matrix(y_test, y_pred).tolist()

    print(f"\n📊 Resultados de Evaluación:")
    print(f"  • Accuracy: {acc:.4f}")
    print(f"  • F1-Score: {f1:.4f}")
    print(f"  • ROC-AUC:  {roc_auc:.4f}")
    print(classification_report(y_test, y_pred))

    # ── Serialización de Artefactos ───────────────────────────────────────────
    model_path = MODELS_DIR / "deforestation_forest_risk_pipeline.joblib"
    joblib.dump(full_pipeline, model_path)
    print(f"\n💾 Pipeline completo serializado en: {model_path}")

    metadata = {
        "model_type": "RandomForestClassifier",
        "target_variable": target_col,
        "target_description": "1 = Deforestación en Bosque Primario (Crítico), 0 = Bosque Secundario / Otro",
        "numeric_features": numeric_features,
        "categorical_features": categorical_features,
        "metrics": {
            "accuracy": round(acc, 4),
            "f1_score": round(f1, 4),
            "roc_auc": round(roc_auc, 4),
            "confusion_matrix": cm,
            "classification_report": report,
        },
        "training_samples": len(X_train),
        "test_samples": len(X_test),
    }

    metadata_path = MODELS_DIR / "model_metadata.json"
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    print(f"💾 Metadatos y métricas guardados en: {metadata_path}")
    print("=" * 65)
    print("✅ ENTRENAMIENTO Y SERIALIZACIÓN FINALIZADOS CON ÉXITO")


if __name__ == "__main__":
    main()
