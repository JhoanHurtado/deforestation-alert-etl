## Datasets ya disponibles en el proyecto

Estos datasets ya han sido descargados y están listos para usarse. No requieren pasos adicionales si `DATA_PROXY_URL` está configurado en `.env` (los notebooks los descargarán automáticamente).

| Dataset | Ruta local | Contenido | Análisis que habilita |
|---------|------------|-----------|----------------------|
| GFW Alertas (Bolivia) | `data/csv/bol_alerts_*.csv` | ~750k filas de alertas de deforestación 2022–2026 con lat/lon, driver, adm1, cobertura forestal | EDA temporal, espacial, causalidad, distancias |
| GFW Alertas (Colombia) | `data/csv/col_alerts_*.csv` | ~800k filas de alertas de deforestación 2022–2026 con lat/lon, driver, adm1, cobertura forestal | EDA temporal, espacial, causalidad, distancias |
| World Bank Indicators | `data/external/worldbank/worldbank_indicators.csv` | PIB, población rural, tierra agrícola, exportaciones agrícolas (BOL+COL, 2015–2024) | Causalidad preliminar, correlaciones económicas |
| FAO FAOSTAT Production | `data/external/faostat/faostat_production.csv` | Producción de soya, ganadería, caña, palma (BOL+COL, 2015–2024) | Composición agrícola, variación YoY |
| GeoNames Places | `data/external/geonames/geonames_places.csv` | ~61k localidades pobladas (BOL+COL) con lat/lon y población | Proximidad a centros poblados, densidad espacial |

---

## Estado de implementación de análisis

| Análisis | Datos requeridos | Estado | Notebook |
|----------|-----------------|--------|----------|
| Tasas de cambio | GFW CSV | ✅ Implementado | `01_eda_gfw.ipynb` §5 |
| Patrones espaciales | GFW CSV | ✅ Implementado | `01_eda_gfw.ipynb` §6 |
| Causalidad preliminar | GFW + WorldBank | ✅ Implementado | `01_eda_gfw.ipynb` §7 |
| Proximidad a poblados | GFW + GeoNames | ✅ Implementado | `01_eda_gfw.ipynb` §8 |
| Análisis extendido WorldBank | WorldBank | ✅ Implementado | `02_eda_worldbank.ipynb` |
| Análisis extendido FAOSTAT | FAOSTAT | ✅ Implementado | `03_eda_faostat.ipynb` |
| Análisis extendido GeoNames | GeoNames | ✅ Implementado | `04_eda_geonames.ipynb` |
| Emisiones de carbono | GFW + Biomasa raster | ⏳ Pendiente (requiere datos biomasa) | — |
| Fragmentación de hábitats | Hansen raster | ⏳ Pendiente (requiere GEE) | — |
| Proximidad carreteras/ríos | OpenStreetMap | ⏳ Pendiente (requiere OSM) | — |
| Impacto en cuencas | HydroSHEDS + CHIRPS | ⏳ Pendiente (requiere descarga adicional) | — |

---

# Datasets externos requeridos para análisis de cruce

Cada sección indica el análisis que habilita, la fuente, el formato descargable y los pasos exactos.

---

## 1. Proximidad a carreteras y ríos

**Análisis**: Medir distancia de alertas de deforestación a carreteras y ríos.

**Fuente**: OpenStreetMap vía Geofabrik  
**URL**: https://download.geofabrik.de/south-america.html

**Pasos**:
1. Ir a https://download.geofabrik.de/south-america.html
2. Descargar los archivos `.osm.pbf` de cada país:
   - Bolivia → clic en `bolivia-latest.osm.pbf`
   - Colombia → clic en `colombia-latest.osm.pbf`
3. Guardar en `data/external/osm/`
4. Extraer carreteras y ríos con `osmium` o `osmfilter`:
   ```bash
   osmium tags-filter bolivia-latest.osm.pbf w/highway -o bolivia_roads.osm.pbf
   osmium tags-filter bolivia-latest.osm.pbf w/waterway -o bolivia_rivers.osm.pbf
   ```
5. Convertir a GeoJSON o Shapefile con `ogr2ogr` para cruzar con lat/lon de alertas.

**Formato final**: GeoJSON o Shapefile (`.shp`)  
**Tamaño aprox.**: Bolivia ~150 MB, Colombia ~300 MB

---

## 2. Proximidad a centros poblados

**Análisis**: Medir distancia de alertas a asentamientos humanos.

**Fuente**: GADM (Database of Global Administrative Areas)  
**URL**: https://gadm.org/download_country.html

**Pasos**:
1. Ir a https://gadm.org/download_country.html
2. Seleccionar país → `Bolivia` → formato `Shapefile` → clic en `Download`
3. Repetir para `Colombia`
4. Guardar en `data/external/gadm/`
5. Usar el nivel ADM3 o ADM4 (municipios/localidades) como proxy de centros poblados.

**Alternativa con mayor detalle de poblados**:  
**Fuente**: GeoNames  
**URL**: https://download.geonames.org/export/dump/  
- Descargar `BO.zip` (Bolivia) y `CO.zip` (Colombia)
- Contiene nombre, lat, lon y tipo de cada localidad

**Formato final**: CSV con lat/lon por localidad  
**Tamaño aprox.**: < 5 MB por país

---

## 3. Fragmentación de hábitats

**Análisis**: Evaluar conectividad ecológica y parches de bosque restante.

**Fuente**: Hansen Global Forest Change (University of Maryland)  
**URL**: https://earthenginepartners.appspot.com/science-2013-global-forest  
**Descarga directa (GEE)**: https://developers.google.com/earth-engine/datasets/catalog/UMD_hansen_global_forest_change_2023_v1_11

**Pasos**:
1. Crear cuenta gratuita en https://earthengine.google.com/
2. Abrir el Code Editor: https://code.earthengine.google.com/
3. Ejecutar el siguiente script para exportar cobertura forestal de Bolivia y Colombia:
   ```javascript
   var hansen = ee.Image('UMD/hansen/global_forest_change_2023_v1_11');
   var treecover = hansen.select('treecover2000');
   var bolivia = ee.FeatureCollection('USDOS/LSIB_SIMPLE/2017')
     .filter(ee.Filter.eq('country_na', 'Bolivia'));
   Export.image.toDrive({
     image: treecover.clip(bolivia),
     description: 'treecover_bolivia',
     scale: 1000,
     region: bolivia.geometry(),
     fileFormat: 'GeoTIFF'
   });
   ```
4. Repetir cambiando `'Bolivia'` por `'Colombia'`
5. Descargar los GeoTIFF desde Google Drive
6. Guardar en `data/external/hansen/`

**Formato final**: GeoTIFF (raster)  
**Tamaño aprox.**: 50–200 MB por país a 1 km de resolución

---

## 4. Emisiones de carbono (biomasa)

**Análisis**: Calcular carbono liberado por deforestación.

**Fuente**: Woods Hole Research Center (WHRC) Pantropical Biomass  
**URL**: https://www.globalforestwatch.org/blog/data-and-research/whrc-aboveground-live-woody-biomass/  
**Descarga directa**: https://opendata.arcgis.com/datasets/091f4230a08f40539b5b8b5e9c8e6b3e_0.zip

**Alternativa más accesible — Global Biomass (ESA CCI)**:  
**URL**: https://climate.esa.int/en/projects/biomass/data/  
**Pasos**:
1. Ir a https://climate.esa.int/en/projects/biomass/data/
2. Registrarse con email institucional o personal
3. Seleccionar año 2020 → región South America → descargar GeoTIFF
4. Guardar en `data/external/biomass/`
5. Cruzar con lat/lon de alertas para extraer toneladas de carbono por píxel

**Fórmula de emisión**:
```
CO2_liberado = biomasa_AGB (tC/ha) × área_deforestada (ha) × 3.67
```
(factor 3.67 convierte toneladas de carbono a toneladas de CO2)

**Formato final**: GeoTIFF  
**Tamaño aprox.**: 500 MB – 2 GB (recortar a AOI reduce a ~100 MB)

---

## 5. Causalidad con variables económicas y agrícolas

**Análisis**: Cruzar pérdida forestal con PIB, producción agrícola, precios de commodities.

### 5a. PIB y demografía por país/departamento

**Fuente**: World Bank Open Data  
**URL**: https://data.worldbank.org/indicator

**Pasos**:
1. Ir a https://data.worldbank.org/indicator/NY.GDP.MKTP.CD
2. Filtrar por país: Bolivia, Colombia
3. Clic en `Download` → seleccionar `CSV`
4. Repetir para indicadores relevantes:
   - Población rural: `SP.RUR.TOTL`
   - Tierra agrícola: `AG.LND.AGRI.ZS`
   - Exportaciones agrícolas: `TX.VAL.AGRI.ZS.UN`
5. Guardar en `data/external/worldbank/`

### 5b. Producción y precios de soya, carne, madera

**Fuente**: FAO FAOSTAT  
**URL**: https://www.fao.org/faostat/en/#data

**Pasos**:
1. Ir a https://www.fao.org/faostat/en/#data/QCL
2. Seleccionar:
   - Countries: Bolivia, Colombia
   - Items: Soybeans, Cattle, Sugar cane
   - Elements: Production quantity, Area harvested
   - Years: 2015–2024
3. Clic en `Download Data` → formato CSV
4. Guardar en `data/external/faostat/`

**Formato final**: CSV  
**Tamaño aprox.**: < 1 MB por indicador

---

## 6. Impacto en cuencas hidrográficas

**Análisis**: Evaluar efectos de deforestación sobre ciclo del agua y erosión.

### 6a. Cuencas hidrográficas (polígonos)

**Fuente**: HydroSHEDS (USGS)  
**URL**: https://www.hydrosheds.org/products/hydrobasins

**Pasos**:
1. Ir a https://www.hydrosheds.org/products/hydrobasins
2. Seleccionar región `South America`
3. Descargar nivel `Level 6` (subcuencas medianas) en formato Shapefile
4. Guardar en `data/external/hydrosheds/`
5. Hacer spatial join con lat/lon de alertas para asignar cada alerta a su cuenca

### 6b. Precipitación y evapotranspiración

**Fuente**: CHIRPS (Climate Hazards Group)  
**URL**: https://www.chc.ucsb.edu/data/chirps

**Pasos**:
1. Ir a https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_monthly/tifs/
2. Descargar archivos `.tif` mensuales para el período 2015–2024
3. Alternativamente usar la API de Google Earth Engine:
   ```javascript
   var chirps = ee.ImageCollection('UCSB-CHG/CHIRPS/MONTHLY')
     .filterDate('2015-01-01', '2024-12-31')
     .filterBounds(bolivia.geometry());
   ```
4. Guardar en `data/external/chirps/`

**Formato final**: GeoTIFF mensual  
**Tamaño aprox.**: ~5 MB por mes × 120 meses = ~600 MB (reducible recortando AOI)

---

## Resumen de estructura de carpetas

```
data/external/
├── osm/              # Carreteras y ríos (OpenStreetMap)
├── gadm/             # Límites administrativos y poblados (GADM)
├── geonames/         # Localidades con lat/lon (GeoNames)
├── hansen/           # Cobertura forestal (Hansen/GEE)
├── biomass/          # Biomasa aérea (ESA CCI o WHRC)
├── worldbank/        # PIB, demografía, tierra agrícola (World Bank)
├── faostat/          # Producción agrícola (FAO)
├── hydrosheds/       # Cuencas hidrográficas (HydroSHEDS)
└── chirps/           # Precipitación mensual (CHIRPS)
```

---

## Requisitos de software para procesamiento

```bash
pip install geopandas rasterio rasterstats fiona pyproj shapely
```

- `geopandas`: spatial joins entre alertas y capas vectoriales
- `rasterio` + `rasterstats`: extraer valores de rasters (biomasa, precipitación) en puntos lat/lon
- `osmium-tool`: filtrar datos OSM (instalar con `brew install osmium-tool`)
