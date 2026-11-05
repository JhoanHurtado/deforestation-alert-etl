"""
deforestation_etl_dag.py — DAG Airflow para el pipeline de alertas de deforestación.

Arquitectura de Datos Moderna (Medallion: Bronze ➔ Quality Gate ➔ Silver/Gold):
  1. Ingesta Multi-Fuente:
     - sync_gfw_alerts (API REST GFW - Incremental)
     - download_worldbank (API REST Banco Mundial)
     - download_faostat (Bulk CSV FAOSTAT)
     - download_geonames (Data Dump GeoNames)
     - scrape_protected_areas (Web Scraping WDPA)
  2. Data Lake / Bronze Layer:
     - upload_to_s3 (persiste la data cruda inmutablemente en S3 antes de procesar)
  3. Control de Calidad Formal (Quality Gate):
     - validate_data_quality (Great Expectations al 100% de datos; bloquea si está vacío o corrupto)
  4. Transformación y Enriquecimiento Atómico (Silver Layer):
     - transform_and_enrich (normalización, derivadas temporales, cálculo esférico BallTree)
  5. Carga al Data Warehouse (Gold Layer / Star Schema):
     - load_data_warehouse (DDL, dimensiones, resolución de FKs, fact_alerts e índices)
  6. Análisis & Visualización de Negocio:
     - generate_analytics_and_viz (ejecuta consultas analíticas y exporta gráficos de negocio)
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

# Detección dinámica de ruta del proyecto: funciona en local (Mac) y en Lightsail
PROJECT_DIR = Path(__file__).resolve().parent.parent
PROJECT = os.getenv("PROJECT_DIR", str(PROJECT_DIR))
PYTHON  = sys.executable

default_args = {
    "owner": "etl-g51",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="deforestation_etl_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 22 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["deforestation", "etl", "ods13", "ods15", "great_expectations", "medallion"],
) as dag:

    # ── Fase 1: Ingesta de Datos (Paralela) ───────────────────────────────────
    sync_gfw = BashOperator(
        task_id="sync_gfw_alerts",
        bash_command=f"{PYTHON} {PROJECT}/scripts/sync_alerts.py --daily",
    )

    dl_worldbank = BashOperator(
        task_id="download_worldbank",
        bash_command=f"{PYTHON} {PROJECT}/scripts/download_worldbank.py",
    )

    dl_faostat = BashOperator(
        task_id="download_faostat",
        bash_command=f"{PYTHON} {PROJECT}/scripts/download_faostat.py",
    )

    dl_geonames = BashOperator(
        task_id="download_geonames",
        bash_command=f"{PYTHON} {PROJECT}/scripts/download_geonames.py",
    )

    scrape_areas = BashOperator(
        task_id="scrape_protected_areas",
        bash_command=f"{PYTHON} {PROJECT}/scripts/scrape_protected_areas.py",
    )

    # ── Fase 2: Data Lake / Bronze Layer (Persistencia Inmutable) ────────────
    upload_s3 = BashOperator(
        task_id="upload_to_s3_bronze",
        bash_command=f"{PYTHON} {PROJECT}/scripts/upload_to_s3.py --keep-local",
    )

    # ── Fase 3: Quality Gate (Great Expectations al 100% de datos) ───────────
    validate_quality = BashOperator(
        task_id="validate_data_quality",
        bash_command=f"{PYTHON} {PROJECT}/scripts/validate_quality.py",
    )

    # ── Fase 4: Transformación & Enriquecimiento Espacial Atómico ────────────
    transform_enrich = BashOperator(
        task_id="transform_and_enrich_data",
        bash_command=f"{PYTHON} {PROJECT}/scripts/transform_and_enrich.py",
    )

    # ── Fase 5: Carga al Data Warehouse (PostgreSQL Star Schema) ─────────────
    load_dwh = BashOperator(
        task_id="load_to_data_warehouse",
        bash_command=f"{PYTHON} {PROJECT}/scripts/load_dwh.py",
    )

    # ── Fase 6: Análisis Analíticos & Dashboards de Negocio ──────────────────
    analytics_viz = BashOperator(
        task_id="generate_analytics_and_viz",
        bash_command=(
            f"cd {PROJECT} && {PROJECT}/venv/bin/jupyter nbconvert "
            f"--to notebook --execute notebooks/05_etl_pipeline.ipynb "
            f"--inplace"
        ),
        execution_timeout=timedelta(hours=1),
    )

    # ── Flujo de Dependencias (DAG Dependencies) ─────────────────────────────
    # 1. Ingesta paralela
    sources = [sync_gfw, dl_worldbank, dl_faostat, dl_geonames, scrape_areas]
    
    # 2. Ingesta ➔ Persistencia Cruda en S3 (Data Lake / Bronze)
    sources >> upload_s3
    
    # 3. Data Lake ➔ Quality Gate (Great Expectations)
    upload_s3 >> validate_quality
    
    # 4. Quality Gate (Solo si pasa) ➔ Transformación Atómica (Silver)
    validate_quality >> transform_enrich
    
    # 5. Transformación ➔ Carga al DWH (Gold)
    transform_enrich >> load_dwh
    
    # 6. DWH ➔ Análisis & Visualizaciones
    load_dwh >> analytics_viz
