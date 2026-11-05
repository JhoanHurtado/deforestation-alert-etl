#!/usr/bin/env python3
"""
load_dwh.py — Tarea atómica de Carga al Data Warehouse (PostgreSQL Star Schema).

Parte del pipeline modular de Airflow:
  1. Ingesta (APIs & Scraping)
  2. Data Lake (upload_to_s3)
  3. Quality Gate (validate_quality con Great Expectations)
  4. transform_and_enrich.py (normalización y distancias esféricas)
  5. load_dwh.py (ESTE SCRIPT):
     - Inicializa el DDL del Data Warehouse con integridad referencial (PKs, FKs, constraints UNIQUE).
     - Carga las dimensiones (dim_date, dim_location, dim_driver, dim_land_cover, dim_confidence,
       dim_economic_context, dim_places).
     - Resuelve claves foráneas y carga la tabla de hechos (fact_alerts) en lotes de 10k filas.
     - Crea índices para optimización de consultas OLAP.
"""

import sys
import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

PROJECT_ROOT = Path(__file__).parent.parent
PROCESSED    = PROJECT_ROOT / "data" / "processed"

PG_USER     = os.getenv('PG_USER', 'etl_user')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'etl_password')
PG_HOST     = os.getenv('PG_HOST', 'localhost')
raw_port    = os.getenv('PG_PORT', '5432')
PG_PORT     = int(raw_port) if str(raw_port).isdigit() else 5432
PG_DB       = os.getenv('PG_DB', 'deforestation_dwh')

DB_URI = f'postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}'

DDL = """
DROP TABLE IF EXISTS fact_alerts          CASCADE;
DROP TABLE IF EXISTS dim_date             CASCADE;
DROP TABLE IF EXISTS dim_location         CASCADE;
DROP TABLE IF EXISTS dim_driver           CASCADE;
DROP TABLE IF EXISTS dim_land_cover       CASCADE;
DROP TABLE IF EXISTS dim_confidence       CASCADE;
DROP TABLE IF EXISTS dim_economic_context CASCADE;
DROP TABLE IF EXISTS dim_places           CASCADE;

CREATE TABLE dim_date (
    date_id    SERIAL     PRIMARY KEY,
    date       DATE       NOT NULL UNIQUE,
    year       SMALLINT   NOT NULL,
    month      SMALLINT   NOT NULL,
    week       SMALLINT   NOT NULL,
    month_name VARCHAR(3) NOT NULL
);

CREATE TABLE dim_location (
    location_id  SERIAL      PRIMARY KEY,
    country_code CHAR(3)     NOT NULL,
    country_name VARCHAR(50) NOT NULL,
    adm1_code    SMALLINT    NOT NULL,
    adm1_name    VARCHAR(60) NOT NULL,
    UNIQUE (country_code, adm1_code)
);

CREATE TABLE dim_driver (
    driver_id   SMALLINT    PRIMARY KEY,
    driver_name VARCHAR(30) NOT NULL
);

CREATE TABLE dim_land_cover (
    land_cover_id    SMALLINT    PRIMARY KEY,
    land_cover_class VARCHAR(60) NOT NULL UNIQUE
);

CREATE TABLE dim_confidence (
    confidence_id      SMALLINT    PRIMARY KEY,
    confidence         VARCHAR(10) NOT NULL UNIQUE,
    is_high_confidence SMALLINT    NOT NULL
);

CREATE TABLE dim_economic_context (
    econ_id              SERIAL   PRIMARY KEY,
    country_code         CHAR(3)  NOT NULL,
    year                 SMALLINT NOT NULL,
    gdp_usd              FLOAT,
    rural_population     FLOAT,
    agricultural_land_pct FLOAT,
    agri_exports_pct     FLOAT,
    soy_production_t     FLOAT,
    soy_area_ha          FLOAT,
    UNIQUE (country_code, year)
);

CREATE TABLE dim_places (
    place_id    SERIAL      PRIMARY KEY,
    geonameid   INT         NOT NULL UNIQUE,
    place_name  VARCHAR(200),
    latitude    FLOAT       NOT NULL,
    longitude   FLOAT       NOT NULL,
    feature_code VARCHAR(10),
    country_code CHAR(3)    NOT NULL,
    admin1_code  VARCHAR(10),
    population  INT
);

CREATE TABLE fact_alerts (
    alert_id               BIGSERIAL PRIMARY KEY,
    date_id                INT       NOT NULL REFERENCES dim_date(date_id),
    location_id            INT       NOT NULL REFERENCES dim_location(location_id),
    driver_id              SMALLINT  NOT NULL REFERENCES dim_driver(driver_id),
    land_cover_id          SMALLINT           REFERENCES dim_land_cover(land_cover_id),
    confidence_id          SMALLINT  NOT NULL REFERENCES dim_confidence(confidence_id),
    econ_id                INT                REFERENCES dim_economic_context(econ_id),
    latitude               FLOAT     NOT NULL,
    longitude              FLOAT     NOT NULL,
    tree_cover_density_pct SMALLINT,
    is_primary_forest      SMALLINT,
    protected_area_cat     SMALLINT,
    is_soy_area            SMALLINT,
    dist_nearest_place_km  FLOAT
);
"""


def main():
    print("=" * 65)
    print("🐘 INICIANDO TAREA ATÓMICA: CARGA AL DATA WAREHOUSE (POSTGRESQL)")
    print(f"   Target: {PG_HOST}:{PG_PORT}/{PG_DB}")
    print("=" * 65)

    try:
        engine = create_engine(DB_URI, echo=False)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
        print("✅ Conexión establecida con PostgreSQL")
    except Exception as e:
        print(f"⚠️ No se pudo conectar a PostgreSQL en {PG_HOST}:{PG_PORT}: {e}")
        print("   (Si estás en modo offline, la carga a DB se simula/omite con éxito).")
        return

    # 1. Crear Esquema
    print("▶ 1. Ejecutando DDL de tablas dimensionales y hechos...")
    with engine.connect() as conn:
        conn.execute(text(DDL))
        conn.commit()
    print("  ✅ Esquema Star Schema creado")

    # 2. Cargar datasets procesados
    print("▶ 2. Cargando datasets procesados desde data/processed/...")
    gfw_path = PROCESSED / "gfw_transformed.pkl"
    wb_path  = PROCESSED / "worldbank_transformed.pkl"
    fao_path = PROCESSED / "faostat_transformed.pkl"
    geo_path = PROCESSED / "geonames_transformed.pkl"

    if not gfw_path.exists():
        print(f"❌ Error: {gfw_path} no encontrado. Ejecuta transform_and_enrich.py primero.")
        sys.exit(1)

    gfw = pd.read_pickle(gfw_path)
    wb  = pd.read_pickle(wb_path) if wb_path.exists() else pd.DataFrame()
    fao = pd.read_pickle(fao_path) if fao_path.exists() else pd.DataFrame()
    geo = pd.read_pickle(geo_path) if geo_path.exists() else pd.DataFrame()

    # 3. Cargar Dimensiones
    print("▶ 3. Poblando tablas de dimensión...")

    # dim_date
    dim_date = (
        gfw[['date']].drop_duplicates()
        .assign(
            year=lambda d: d['date'].dt.year,
            month=lambda d: d['date'].dt.month,
            week=lambda d: d['date'].dt.isocalendar().week.astype(int),
            month_name=lambda d: d['date'].dt.strftime('%b'),
        )
        .sort_values('date')
        .reset_index(drop=True)
    )
    dim_date.index.name = 'date_id'
    dim_date = dim_date.reset_index()
    dim_date['date_id'] += 1
    dim_date.to_sql('dim_date', engine, if_exists='append', index=False, chunksize=5000, method='multi')
    print(f"  dim_date: {len(dim_date):,} filas")

    # dim_location
    dim_location = (
        gfw[['country_code', 'country_name', 'adm1_code', 'adm1_name']]
        .drop_duplicates()
        .sort_values(['country_code', 'adm1_code'])
        .reset_index(drop=True)
    )
    dim_location.index.name = 'location_id'
    dim_location = dim_location.reset_index()
    dim_location['location_id'] += 1
    dim_location.to_sql('dim_location', engine, if_exists='append', index=False, chunksize=5000, method='multi')
    print(f"  dim_location: {len(dim_location):,} filas")

    # dim_driver
    dim_driver = (
        gfw[['loss_driver_category', 'loss_driver']].drop_duplicates()
        .rename(columns={'loss_driver_category': 'driver_id', 'loss_driver': 'driver_name'})
        .dropna(subset=['driver_id'])
        .astype({'driver_id': int})
        .sort_values('driver_id')
    )
    dim_driver.to_sql('dim_driver', engine, if_exists='append', index=False, chunksize=5000, method='multi')
    print(f"  dim_driver: {len(dim_driver):,} filas")

    # dim_land_cover
    dim_land_cover = (
        gfw[['land_cover_class']].drop_duplicates()
        .dropna()
        .sort_values('land_cover_class')
        .reset_index(drop=True)
    )
    dim_land_cover.index.name = 'land_cover_id'
    dim_land_cover = dim_land_cover.reset_index()
    dim_land_cover['land_cover_id'] += 1
    dim_land_cover.to_sql('dim_land_cover', engine, if_exists='append', index=False, chunksize=5000, method='multi')
    print(f"  dim_land_cover: {len(dim_land_cover):,} filas")

    # dim_confidence
    dim_confidence = (
        gfw[['confidence', 'is_high_conf']].drop_duplicates()
        .rename(columns={'is_high_conf': 'is_high_confidence'})
        .reset_index(drop=True)
    )
    dim_confidence.index.name = 'confidence_id'
    dim_confidence = dim_confidence.reset_index()
    dim_confidence['confidence_id'] += 1
    dim_confidence.to_sql('dim_confidence', engine, if_exists='append', index=False, chunksize=5000, method='multi')
    print(f"  dim_confidence: {len(dim_confidence):,} filas")

    # dim_economic_context
    if not wb.empty:
        econ = wb.copy()
    else:
        econ = pd.DataFrame(columns=['country_code', 'year'])

    if not fao.empty:
        fao_soy = fao[fao['item_name'] == 'Soybeans'][['country_code', 'year', 'production_t', 'area_ha']].copy()
        fao_soy = fao_soy.rename(columns={'production_t': 'soy_production_t', 'area_ha': 'soy_area_ha'})
        econ = econ.merge(fao_soy, on=['country_code', 'year'], how='outer') if not econ.empty else fao_soy

    if not econ.empty:
        econ = econ.reset_index(drop=True)
        econ.index.name = 'econ_id'
        econ = econ.reset_index()
        econ['econ_id'] += 1
        econ.to_sql('dim_economic_context', engine, if_exists='append', index=False, chunksize=5000, method='multi')
        print(f"  dim_economic_context: {len(econ):,} filas")
    else:
        print("  ⚠️ dim_economic_context omitido")

    # dim_places
    if not geo.empty:
        geo_dim = geo.reset_index(drop=True)
        geo_dim.index.name = 'place_id'
        geo_dim = geo_dim.reset_index()
        geo_dim['place_id'] += 1
        geo_dim.to_sql('dim_places', engine, if_exists='append', index=False, chunksize=5000, method='multi')
        print(f"  dim_places: {len(geo_dim):,} filas")

    # 4. Construir y Cargar fact_alerts
    print("▶ 4. Mapeando claves foráneas y cargando fact_alerts...")
    date_map = dim_date.set_index('date')['date_id']
    loc_map  = dim_location.set_index(['country_code', 'adm1_code'])['location_id']
    lc_map   = dim_land_cover.set_index('land_cover_class')['land_cover_id']
    conf_map = dim_confidence.set_index('confidence')['confidence_id']
    econ_map = econ.set_index(['country_code', 'year'])['econ_id'] if not econ.empty else pd.Series(dtype=int)

    fact = gfw.copy()
    fact['date_id']       = fact['date'].map(date_map)
    fact['location_id']   = pd.MultiIndex.from_arrays([fact['country_code'], fact['adm1_code']]).map(loc_map)
    fact['driver_id']     = fact['loss_driver_category'].astype('Int64')
    fact['land_cover_id'] = fact['land_cover_class'].map(lc_map)
    fact['confidence_id'] = fact['confidence'].map(conf_map)
    fact['econ_id']       = pd.MultiIndex.from_arrays([fact['country_code'], fact['year']]).map(econ_map) if not econ_map.empty else None

    fact_alerts = fact[[
        'date_id', 'location_id', 'driver_id', 'land_cover_id', 'confidence_id', 'econ_id',
        'latitude', 'longitude', 'tree_cover_density_pct', 'is_primary_forest',
        'protected_area_cat', 'is_soy_area', 'dist_nearest_place_km'
    ]].copy()

    fact_alerts.to_sql('fact_alerts', engine, if_exists='append', index=False, chunksize=10000, method='multi')
    print(f"  ✅ fact_alerts cargada exitosamente: {len(fact_alerts):,} filas")

    # 5. Crear Índices de Rendimiento
    print("▶ 5. Creando índices en PostgreSQL...")
    with engine.connect() as conn:
        for name, col in [
            ('idx_fact_date',       'fact_alerts(date_id)'),
            ('idx_fact_location',   'fact_alerts(location_id)'),
            ('idx_fact_driver',     'fact_alerts(driver_id)'),
            ('idx_fact_confidence', 'fact_alerts(confidence_id)'),
            ('idx_fact_econ',       'fact_alerts(econ_id)'),
        ]:
            conn.execute(text(f'CREATE INDEX IF NOT EXISTS {name} ON {col}'))
        conn.commit()
    print("  ✅ Índices creados")

    print("=" * 65)
    print("🏁 CARGA AL DATA WAREHOUSE COMPLETADA CON ÉXITO")


if __name__ == "__main__":
    main()

