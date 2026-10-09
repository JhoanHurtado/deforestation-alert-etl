#!/usr/bin/env python3
"""
validate_quality.py — Validación automatizada de calidad de datos con Great Expectations.

Requisito de la Segunda Entrega (Rubro: Data quality with Great Expectations):
  - Evalúa la calidad de los datos crudos extraídos de las 4 fuentes antes de cargarlos a PostgreSQL.
  - Define suites de validación (Expectation Suites) para GFW, World Bank, GeoNames y Áreas Protegidas.
  - Registra el informe en data/logs/great_expectations.log.
  - Solo permite continuar el pipeline en Airflow si todas las validaciones críticas son exitosas (exit code 0).
"""

import sys
import os
import pandas as pd
from pathlib import Path
from datetime import datetime

# Rutas del proyecto
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
LOG_DIR      = DATA_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE     = LOG_DIR / "great_expectations.log"

try:
    import great_expectations as gx
except ImportError:
    print("❌ great_expectations no está instalado. Ejecuta: pip install great-expectations")
    sys.exit(1)


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def validate_dataframe(context, df: pd.DataFrame, suite_name: str, expectations_list: list) -> bool:
    """Ejecuta una lista de expectativas de Great Expectations sobre un DataFrame de Pandas."""
    if df.empty:
        log(f"  ⚠️  Dataset para {suite_name} está vacío. Validación omitida.")
        return True

    ds = context.data_sources.add_pandas(f"source_{suite_name}")
    asset = ds.add_dataframe_asset(f"asset_{suite_name}")
    batch_def = asset.add_batch_definition_whole_dataframe(f"batch_{suite_name}")
    
    suite = context.suites.add(gx.ExpectationSuite(name=f"suite_{suite_name}"))
    for exp in expectations_list:
        suite.add_expectation(exp)
        
    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"val_{suite_name}", data=batch_def, suite=suite)
    )
    result = val_def.run(batch_parameters={"dataframe": df})
    
    log(f"  ▶ Suite '{suite_name}': {'PASÓ ✅' if result.success else 'FALLÓ ❌'}")
    return result.success


def main():
    log("=" * 60)
    log("🚀 INICIANDO SUITE DE VALIDACIÓN CON GREAT EXPECTATIONS")
    log(f"   GX Version: {gx.__version__}")
    log("=" * 60)

    context = gx.get_context(mode="ephemeral")
    all_passed = True

    # ── 1. Validación de Alertas de Deforestación GFW ──────────────────────────
    # Limpiar posibles archivos temporales de 0 bytes
    for p in (DATA_DIR / "csv").glob("*alerts*.csv"):
        if p.stat().st_size == 0:
            try:
                p.unlink()
                log(f"  🗑️ Archivo vacío de 0 bytes eliminado: {p.name}")
            except OSError:
                pass

    gfw_files = [p for p in (DATA_DIR / "csv").glob("*alerts*.csv") if p.stat().st_size > 0]
    if not gfw_files:
        log("⚠️ No se encontraron archivos GFW válidos en data/csv. Descargando base desde proxy S3...")
        try:
            import subprocess
            fetch_script = PROJECT_ROOT / "scripts" / "fetch_from_proxy.py"
            if fetch_script.exists():
                subprocess.run([sys.executable, str(fetch_script), "--only", "gfw"], check=True)
                gfw_files = [p for p in (DATA_DIR / "csv").glob("*alerts*.csv") if p.stat().st_size > 0]
        except Exception as e:
            log(f"⚠️ Error al obtener CSVs base desde el proxy: {e}")

    if gfw_files:
        for gfw_path in gfw_files:
            log(f"Validando GFW: {gfw_path.name}")
            try:
                df_gfw = pd.read_csv(gfw_path, nrows=10000)  # Validar muestra representativa de 10k filas
            except pd.errors.EmptyDataError:
                log(f"  ⚠️ Archivo {gfw_path.name} no contiene columnas ni datos. Omitiendo.")
                continue
            
            exp_gfw = [
                gx.expectations.ExpectColumnValuesToNotBeNull(column="latitude"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="longitude"),
                gx.expectations.ExpectColumnValuesToBeBetween(column="latitude", min_value=-90.0, max_value=90.0),
                gx.expectations.ExpectColumnValuesToBeBetween(column="longitude", min_value=-180.0, max_value=180.0),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="gfw_integrated_alerts__date"),
                gx.expectations.ExpectColumnValuesToBeBetween(column="umd_tree_cover_density_2000__percent", min_value=0, max_value=100),
                gx.expectations.ExpectColumnValuesToBeInSet(column="gfw_integrated_alerts__confidence", value_set=["nominal", "high", "highest"]),
            ]
            suite_id = f"gfw_{gfw_path.stem}"
            if not validate_dataframe(context, df_gfw, suite_id, exp_gfw):
                all_passed = False
    else:
        log("⚠️ No se encontraron archivos GFW en data/csv para validar.")

    # ── 2. Validación de World Bank ───────────────────────────────────────────
    wb_path = DATA_DIR / "external" / "worldbank" / "worldbank_indicators.csv"
    if wb_path.exists() and wb_path.stat().st_size > 0:
        log(f"Validando World Bank: {wb_path.name}")
        try:
            df_wb = pd.read_csv(wb_path)
            exp_wb = [
                gx.expectations.ExpectColumnValuesToNotBeNull(column="country_code"),
                gx.expectations.ExpectColumnValuesToBeInSet(column="country_code", value_set=["BOL", "COL"]),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="indicator_name"),
                gx.expectations.ExpectColumnValuesToBeBetween(column="year", min_value=2000, max_value=2030),
            ]
            if not validate_dataframe(context, df_wb, "worldbank", exp_wb):
                all_passed = False
        except pd.errors.EmptyDataError:
            log(f"  ⚠️ Archivo {wb_path.name} está vacío. Omitiendo.")

    # ── 3. Validación de GeoNames ─────────────────────────────────────────────
    geo_path = DATA_DIR / "external" / "geonames" / "geonames_places.csv"
    if geo_path.exists() and geo_path.stat().st_size > 0:
        log(f"Validando GeoNames: {geo_path.name}")
        try:
            df_geo = pd.read_csv(geo_path, nrows=5000)
            exp_geo = [
                gx.expectations.ExpectColumnValuesToNotBeNull(column="geonameid"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="latitude"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="longitude"),
                gx.expectations.ExpectColumnValuesToBeBetween(column="latitude", min_value=-90.0, max_value=90.0),
                gx.expectations.ExpectColumnValuesToBeBetween(column="longitude", min_value=-180.0, max_value=180.0),
            ]
            if not validate_dataframe(context, df_geo, "geonames", exp_geo):
                all_passed = False
        except pd.errors.EmptyDataError:
            log(f"  ⚠️ Archivo {geo_path.name} está vacío. Omitiendo.")

    # ── 4. Validación de Áreas Protegidas ─────────────────────────────────────
    pa_path = DATA_DIR / "external" / "protected_areas" / "protected_areas.csv"
    if pa_path.exists() and pa_path.stat().st_size > 0:
        log(f"Validando Áreas Protegidas: {pa_path.name}")
        try:
            df_pa = pd.read_csv(pa_path)
            exp_pa = [
                gx.expectations.ExpectColumnValuesToNotBeNull(column="name"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="country_code"),
                gx.expectations.ExpectColumnValuesToBeInSet(column="country_code", value_set=["BOL", "COL"]),
            ]
            if not validate_dataframe(context, df_pa, "protected_areas", exp_pa):
                all_passed = False
        except pd.errors.EmptyDataError:
            log(f"  ⚠️ Archivo {pa_path.name} está vacío. Omitiendo.")

    log("=" * 60)
    if all_passed:
        log("✅ TODAS LAS VALIDACIONES DE GREAT EXPECTATIONS FUERON EXITOSAS.")
        log("   Los datos cumplen con los umbrales de calidad para ser cargados al Data Warehouse.")
        sys.exit(0)
    else:
        log("❌ SE DETECTARON VIOLACIONES DE CALIDAD DE DATOS.")
        log("   La carga a PostgreSQL ha sido bloqueada según la política de calidad.")
        sys.exit(1)


if __name__ == "__main__":
    main()

