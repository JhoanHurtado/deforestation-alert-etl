#!/usr/bin/env python3
"""
transform_and_enrich.py — Tarea atómica de Transformación y Enriquecimiento.

Parte del pipeline modular de Airflow:
  1. Ingesta (APIs & Scraping)
  2. Data Lake (upload_to_s3)
  3. Quality Gate (validate_quality con Great Expectations)
  4. transform_and_enrich.py (ESTE SCRIPT):
     - Limpieza y normalización de alertas GFW (fechas, drivers, confianza, departamentos GADM).
     - Pivot y estandarización de World Bank y FAOSTAT.
     - Limpieza de catálogo de GeoNames.
     - Enriquecimiento espacial: cálculo de distancias mínimas esféricas a localidades
       (BallTree con métrica Haversine en radianes).
     - Serializa los datasets transformados en data/processed/ para la carga atómica.
"""

import sys
import os
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.neighbors import BallTree

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
PROCESSED    = DATA_DIR / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)

COUNTRIES = {'BOL': 'Bolivia', 'COL': 'Colombia'}

DRIVER_MAP = {
    1: 'Agricultura', 2: 'Ganaderia', 3: 'Silvicultura', 4: 'Mineria',
    5: 'Infraestructura', 6: 'Incendios', 7: 'Otro', 255: 'Sin clasificar',
}

ADM1_BOL = {
    1: 'Chuquisaca', 2: 'Cochabamba', 3: 'Beni', 4: 'La Paz',
    6: 'Pando', 8: 'Santa Cruz', 9: 'Tarija',
}
ADM1_COL = {
    1: 'Amazonas', 2: 'Antioquia', 3: 'Arauca', 4: 'Atlantico', 5: 'Bolivar',
    6: 'Boyaca', 7: 'Caldas', 8: 'Caqueta', 9: 'Casanare', 10: 'Cauca',
    11: 'Cesar', 12: 'Choco', 13: 'Cordoba', 14: 'Cundinamarca', 15: 'Guainia',
    16: 'Guaviare', 17: 'Huila', 18: 'La Guajira', 19: 'Magdalena', 20: 'Meta',
    21: 'Narino', 22: 'Norte de Santander', 23: 'Putumayo', 24: 'Quindio',
    25: 'Risaralda', 26: 'San Andres', 27: 'Santander', 28: 'Sucre',
    29: 'Tolima', 30: 'Valle del Cauca', 31: 'Vaupes', 32: 'Vichada', 33: 'Bogota D.C.',
}
ADM1_MAP = {'BOL': ADM1_BOL, 'COL': ADM1_COL}


def main():
    print("=" * 65)
    print("⚙️ INICIANDO TAREA ATÓMICA: TRANSFORMACIÓN Y ENRIQUECIMIENTO")
    print("=" * 65)

    # 1. Transformar GFW
    print("▶ 1. Procesando y limpiando alertas GFW...")
    gfw_frames = []
    for code in COUNTRIES:
        matches = list((DATA_DIR / 'csv').glob(f'{code.lower()}_alerts_*.csv'))
        if matches:
            df = pd.read_csv(matches[0])
            df['country_code'] = code
            df['country_name'] = COUNTRIES[code]
            gfw_frames.append(df)
            print(f"  Alertas cargadas para {code}: {len(df):,} filas")

    if not gfw_frames:
        print("❌ Error fatal: no se encontraron archivos GFW en data/csv")
        sys.exit(1)

    gfw_raw = pd.concat(gfw_frames, ignore_index=True)
    gfw_raw['gfw_integrated_alerts__date'] = pd.to_datetime(gfw_raw['gfw_integrated_alerts__date'])

    gfw = gfw_raw.rename(columns={
        'gfw_integrated_alerts__date':                  'date',
        'gfw_integrated_alerts__confidence':            'confidence',
        'gfw_integrated_alerts__intensity':             'intensity',
        'umd_tree_cover_density_2000__percent':         'tree_cover_density_pct',
        'is__umd_regional_primary_forest_2001':         'is_primary_forest',
        'wdpa_protected_areas__iucn_cat':               'protected_area_cat',
        'esa_land_cover_2015__class':                   'land_cover_class',
        'wri_google_tree_cover_loss_drivers__category': 'loss_driver_category',
        'gadm_administrative_boundaries__adm1':         'adm1_code',
        'is__umd_soy_planted_area_buffered_10km':       'is_soy_area',
    }).copy()

    gfw['year']               = gfw['date'].dt.year.astype('int16')
    gfw['month']              = gfw['date'].dt.month.astype('int8')
    gfw['week']               = gfw['date'].dt.isocalendar().week.astype('int8')
    gfw['month_name']         = gfw['date'].dt.strftime('%b')
    gfw['is_high_confidence'] = gfw['confidence'].isin(['high', 'highest']).astype(int)
    gfw['country_name']       = gfw['country_code'].map(COUNTRIES)
    gfw['adm1_code']          = gfw['adm1_code'].fillna(-1).astype('int16')
    gfw['adm1_name']          = gfw.apply(
        lambda r: ADM1_MAP[r['country_code']].get(r['adm1_code'], 'Desconocido'), axis=1
    )
    gfw['loss_driver']        = gfw['loss_driver_category'].map(DRIVER_MAP).fillna('Sin clasificar')
    gfw['land_cover_class']   = gfw['land_cover_class'].fillna('Desconocido')
    print(f"  Total alertas GFW normalizadas: {len(gfw):,}")

    # 2. Transformar World Bank (Pivot largo a ancho)
    print("▶ 2. Procesando indicadores macroeconómicos de World Bank...")
    wb_path = DATA_DIR / 'external' / 'worldbank' / 'worldbank_indicators.csv'
    if wb_path.exists() and wb_path.stat().st_size > 0:
        wb_raw = pd.read_csv(wb_path)
        wb = wb_raw.pivot_table(
            index=['country_code', 'year'],
            columns='indicator_name',
            values='value'
        ).reset_index()
        wb.columns.name = None
        print(f"  World Bank pivoteado: {len(wb)} registros anuales")
    else:
        wb = pd.DataFrame(columns=['country_code', 'year'])
        print("  ⚠️ World Bank no disponible")

    # 3. Transformar FAOSTAT
    print("▶ 3. Procesando estadísticas agropecuarias de FAOSTAT...")
    fao_path = DATA_DIR / 'external' / 'faostat' / 'faostat_production.csv'
    if fao_path.exists() and fao_path.stat().st_size > 0:
        fao_raw = pd.read_csv(fao_path)
        fao = fao_raw.pivot_table(
            index=['country_code', 'country_name', 'year', 'item_name'],
            columns='element',
            values='value'
        ).reset_index()
        fao.columns.name = None
        col_map = {}
        if 'production_tonnes' in fao.columns:
            col_map['production_tonnes'] = 'production_t'
        if 'area_harvested_ha' in fao.columns:
            col_map['area_harvested_ha'] = 'area_ha'
        fao = fao.rename(columns=col_map)
        print(f"  FAOSTAT pivoteado: {len(fao)} registros")
    else:
        fao = pd.DataFrame(columns=['country_code', 'year'])
        print("  ⚠️ FAOSTAT no disponible")

    # 4. Transformar GeoNames y calcular BallTree Haversine
    print("▶ 4. Indexación espacial esférica (BallTree Haversine) con GeoNames...")
    geo_path = DATA_DIR / 'external' / 'geonames' / 'geonames_places.csv'
    if geo_path.exists() and geo_path.stat().st_size > 0:
        geo_raw = pd.read_csv(geo_path)
        geo = geo_raw[['geonameid', 'name', 'latitude', 'longitude',
                       'feature_code', 'country_iso3', 'admin1_code', 'population']].copy()
        geo = geo.rename(columns={'name': 'place_name', 'country_iso3': 'country_code'})
        geo = geo.dropna(subset=['latitude', 'longitude'])

        # BallTree sobre la esfera terrestre en radianes (R = 6371.0088 km)
        places_rad = np.radians(geo[['latitude', 'longitude']].values)
        tree_places = BallTree(places_rad, metric='haversine')

        alerts_rad = np.radians(gfw[['latitude', 'longitude']].values)
        dists_rad, _ = tree_places.query(alerts_rad, k=1)
        gfw['dist_nearest_place_km'] = (dists_rad.flatten() * 6371.0088).round(2)
        print(f"  Distancias calculadas para {len(gfw):,} alertas. Media: {gfw['dist_nearest_place_km'].mean():.2f} km")
    else:
        geo = pd.DataFrame()
        gfw['dist_nearest_place_km'] = np.nan
        print("  ⚠️ GeoNames no disponible")

    # 5. Guardar datasets procesados para la carga atómica
    print("▶ 5. Guardando datasets estructurados en data/processed/...")
    gfw.to_pickle(PROCESSED / "gfw_transformed.pkl")
    wb.to_pickle(PROCESSED / "worldbank_transformed.pkl")
    fao.to_pickle(PROCESSED / "faostat_transformed.pkl")
    if not geo.empty:
        geo.to_pickle(PROCESSED / "geonames_transformed.pkl")

    print("=" * 65)
    print("✅ TAREA DE TRANSFORMACIÓN Y ENRIQUECIMIENTO COMPLETADA CON ÉXITO")
    print(f"   Archivos generados en {PROCESSED}")


if __name__ == "__main__":
    main()
