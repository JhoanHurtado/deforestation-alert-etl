#!/usr/bin/env python3
"""
validate_quality.py — Validación automatizada de calidad de datos con Great Expectations.

Requisitos de Calidad de Datos (Quality Gate):
  - Evalúa la calidad de los datasets crudos extraídos de las 4 fuentes (GFW, World Bank, FAOSTAT, GeoNames, Áreas Protegidas).
  - VALIDA LA TOTALIDAD DE LOS DATOS (sin submuestreo artificial).
  - RECHAZA DATASETS VACÍOS: Un dataset de 0 filas o inexistente marca FALLA CRÍTICA (exit 1).
  - Bloquea la carga al Data Warehouse si alguna expectativa no se cumple (exit code 1).
  - Registra el reporte en data/logs/great_expectations.log.
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
    """
    Ejecuta una lista de expectativas de Great Expectations sobre un DataFrame de Pandas.
    Si el DataFrame está vacío, la validación FALLA inmediatamente (retorna False).
    """
    if df is None or df.empty or len(df) == 0:
        log(f"  ❌ Suite '{suite_name}': DATASET VACÍO (0 filas). Calidad RECHAZADA.")
        return False

    ds = context.data_sources.add_pandas(f"source_{suite_name}")
    asset = ds.add_dataframe_asset(f"asset_{suite_name}")
    batch_def = asset.add_batch_definition_whole_dataframe(f"batch_{suite_name}")
    
    suite = context.suites.add(gx.ExpectationSuite(name=f"suite_{suite_name}"))
    # Validación universal: el dataset debe tener al menos 1 fila
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1))
    for exp in expectations_list:
        suite.add_expectation(exp)
        
    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"val_{suite_name}", data=batch_def, suite=suite)
    )
    result = val_def.run(batch_parameters={"dataframe": df})
    
    status = "PASÓ ✅" if result.success else "FALLÓ ❌"
    log(f"  ▶ Suite '{suite_name}' ({len(df):,} filas): {status}")
    return bool(result.success)


def main():
    log("=" * 65)
    log("🚀 INICIANDO SUITE DE VALIDACIÓN CON GREAT EXPECTATIONS (QUALITY GATE)")
    log(f"   GX Version: {gx.__version__} | Modo: Dataset Completo (100% de datos)")
    log("=" * 65)

    context = gx.get_context(mode="ephemeral")
    all_passed = True
    validated_sources = 0

    # ── 1. Validación de Alertas de Deforestación GFW (Dataset Completo) ───────
    # Limpiar posibles archivos temporales vacíos
    for p in (DATA_DIR / "csv").glob("*alerts*.csv"):
        if p.stat().st_size == 0:
            try:
                p.unlink()
                log(f"  🗑️ Archivo temporal de 0 bytes eliminado: {p.name}")
            except OSError:
                pass

    gfw_files = [p for p in (DATA_DIR / "csv").glob("*alerts*.csv") if p.stat().st_size > 0]
    if not gfw_files:
        log("⚠️ No se encontraron archivos GFW locales. Intentando sincronizar desde Data Lake / Proxy...")
        try:
            import subprocess
            fetch_script = PROJECT_ROOT / "scripts" / "fetch_from_proxy.py"
            if fetch_script.exists():
                subprocess.run([sys.executable, str(fetch_script), "--only", "gfw"], check=True)
                gfw_files = [p for p in (DATA_DIR / "csv").glob("*alerts*.csv") if p.stat().st_size > 0]
        except Exception as e:
            log(f"⚠️ Error al obtener CSVs base desde el proxy: {e}")

    if not gfw_files:
        log("❌ RECHAZADO: No hay archivos GFW en data/csv. Pipeline detenido.")
        all_passed = False
    else:
        for gfw_path in gfw_files:
            log(f"Validando GFW completo: {gfw_path.name}")
            try:
                # Validar el 100% del dataset sin recortes ni muestreos
                df_gfw = pd.read_csv(gfw_path)
            except Exception as e:
                log(f"  ❌ Error al leer {gfw_path.name}: {e}")
                all_passed = False
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
            validated_sources += 1

    # ── 2. Validación de World Bank ───────────────────────────────────────────
    wb_path = DATA_DIR / "external" / "worldbank" / "worldbank_indicators.csv"
    if not wb_path.exists() or wb_path.stat().st_size == 0:
        log(f"❌ RECHAZADO: {wb_path.name} no existe o está vacío.")
        all_passed = False
    else:
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
            validated_sources += 1
        except Exception as e:
            log(f"  ❌ Error leyendo {wb_path.name}: {e}")
            all_passed = False

    # ── 3. Validación de FAOSTAT ──────────────────────────────────────────────
    fao_path = DATA_DIR / "external" / "faostat" / "faostat_production.csv"
    if not fao_path.exists() or fao_path.stat().st_size == 0:
        log(f"❌ RECHAZADO: {fao_path.name} no existe o está vacío.")
        all_passed = False
    else:
        log(f"Validando FAOSTAT: {fao_path.name}")
        try:
            df_fao = pd.read_csv(fao_path)
            exp_fao = [
                gx.expectations.ExpectColumnValuesToNotBeNull(column="country_code"),
                gx.expectations.ExpectColumnValuesToBeInSet(column="country_code", value_set=["BOL", "COL"]),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="item_name"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="element"),
                gx.expectations.ExpectColumnValuesToBeBetween(column="year", min_value=2000, max_value=2030),
            ]
            if not validate_dataframe(context, df_fao, "faostat", exp_fao):
                all_passed = False
            validated_sources += 1
        except Exception as e:
            log(f"  ❌ Error leyendo {fao_path.name}: {e}")
            all_passed = False

    # ── 4. Validación de GeoNames (Dataset Completo) ───────────────────────────
    geo_path = DATA_DIR / "external" / "geonames" / "geonames_places.csv"
    if not geo_path.exists() or geo_path.stat().st_size == 0:
        log(f"❌ RECHAZADO: {geo_path.name} no existe o está vacío.")
        all_passed = False
    else:
        log(f"Validando GeoNames completo: {geo_path.name}")
        try:
            df_geo = pd.read_csv(geo_path)
            exp_geo = [
                gx.expectations.ExpectColumnValuesToNotBeNull(column="geonameid"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="latitude"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="longitude"),
                gx.expectations.ExpectColumnValuesToBeBetween(column="latitude", min_value=-90.0, max_value=90.0),
                gx.expectations.ExpectColumnValuesToBeBetween(column="longitude", min_value=-180.0, max_value=180.0),
            ]
            if not validate_dataframe(context, df_geo, "geonames", exp_geo):
                all_passed = False
            validated_sources += 1
        except Exception as e:
            log(f"  ❌ Error leyendo {geo_path.name}: {e}")
            all_passed = False

    # ── 5. Validación de Áreas Protegidas ─────────────────────────────────────
    pa_path = DATA_DIR / "external" / "protected_areas" / "protected_areas.csv"
    if not pa_path.exists() or pa_path.stat().st_size == 0:
        log(f"❌ RECHAZADO: {pa_path.name} no existe o está vacío.")
        all_passed = False
    else:
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
            validated_sources += 1
        except Exception as e:
            log(f"  ❌ Error leyendo {pa_path.name}: {e}")
            all_passed = False

    log("=" * 65)
    if all_passed and validated_sources >= 5:
        log(f"✅ TODAS LAS VALIDACIONES ({validated_sources} suites) FUERON EXITOSAS.")
        log("   Los datos cumplen con las expectativas de completitud y calidad.")
        log("   Quality Gate: APROBADO ➔ Autorizada la carga al Data Warehouse.")
        sys.exit(0)
    else:
        log("❌ SE DETECTARON VIOLACIONES CRÍTICAS DE CALIDAD O DATASETS VACÍOS/FALTANTES.")
        log("   Quality Gate: RECHAZADO ➔ Bloqueando ejecución y abortando carga a PostgreSQL.")
        sys.exit(1)


if __name__ == "__main__":
    main()
