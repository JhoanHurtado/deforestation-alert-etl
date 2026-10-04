# 🌳 Deforestation Alert ETL — Segunda Entrega

Pipeline ETL automatizado para el análisis de alertas de deforestación en Bolivia y Colombia (2022–2026).  
Proyecto académico — **ETL (G51)** · Ingeniería de Datos e Inteligencia Artificial.

Alineado con **ODS 13 — Acción por el clima** y **ODS 15 — Vida de ecosistemas terrestres**.

---

## Tabla de contenidos

- [Arquitectura](#arquitectura)
- [Fuentes de datos](#fuentes-de-datos)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Scripts — qué hace cada uno](#scripts--qué-hace-cada-uno)
- [Notebooks — orden de ejecución](#notebooks--orden-de-ejecución)
- [Instalación y configuración](#instalación-y-configuración)
- [Cómo ejecutar el pipeline](#cómo-ejecutar-el-pipeline)
- [Despliegue en AWS Lightsail](#despliegue-en-aws-lightsail)
- [GitHub Actions CI/CD](#github-actions-cicd)
- [Modelo de datos](#modelo-de-datos-star-schema)
- [Alineación con los ODS](#alineación-con-los-ods)

---

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────────────┐
│  DATA SOURCES          LIGHTSAIL (batch)        AWS S3 (raw storage)   │
│                                                                         │
│  GFW API ──────────→  sync_alerts.py ──────┐                           │
│  World Bank API ───→  download_worldbank.py ├──→ upload_to_s3.py ──→  │
│  FAO FAOSTAT ──────→  download_faostat.py  ├──→  raw/gfw/             │
│  GeoNames dump ────→  download_geonames.py ┘     raw/external/        │
│                       (orquestado por                                   │
│                        batch_download.py)         ↓ Reverse Proxy      │
└───────────────────────────────────────────────────┼─────────────────────┘
                                                    │
                                          fetch_from_proxy.py
                                                    │
┌───────────────────────────────────────────────────▼─────────────────────┐
│  LOCAL — Jupyter Notebooks                                               │
│                                                                         │
│  01_eda_gfw.ipynb        EDA raw GFW → decisiones de transformación     │
│  02_eda_worldbank.ipynb  EDA indicadores económicos                     │
│  03_eda_faostat.ipynb    EDA producción agrícola                        │
│  04_eda_geonames.ipynb   EDA localidades pobladas                       │
│           ↓                                                             │
│  05_etl_pipeline.ipynb   ETL unificado + carga incremental              │
│           ↓                                                             │
│  PostgreSQL (Star Schema) → Visualizaciones                             │
└─────────────────────────────────────────────────────────────────────────┘

GitHub Actions:
  deploy.yml  → push a main → SSH → actualiza scripts + cron en Lightsail
  batch.yml   → cron 06:00 UTC → SSH → ejecuta batch_download.py
```

El diagrama editable está en [`docs/architecture.drawio`](docs/architecture.drawio).

---

## Fuentes de datos

| # | Fuente | Tipo | Contenido | Script |
|---|--------|------|-----------|--------|
| 1 | [Global Forest Watch](https://data-api.globalforestwatch.org) | API REST | Alertas de deforestación BOL+COL 2022–2026 (~1.5M filas) | `sync_alerts.py` |
| 2 | [World Bank Open Data](https://data.worldbank.org) | API REST | PIB, población rural, tierra agrícola, exportaciones agrícolas | `download_worldbank.py` |
| 3 | [FAO FAOSTAT](https://www.fao.org/faostat) | Bulk CSV | Producción de soya, ganadería, caña, palma (2015–2024) | `download_faostat.py` |
| 4 | [GeoNames](https://download.geonames.org/export/dump/) | ZIP/CSV | ~61k localidades pobladas BOL+COL con coordenadas | `download_geonames.py` |

---

## Estructura del proyecto

```
deforestation-alert-etl/
├── .github/
│   └── workflows/
│       ├── deploy.yml          # CI/CD: deploy scripts a Lightsail en push a main
│       └── batch.yml           # Cron diario: ejecuta batch_download.py en Lightsail
├── data/
│   ├── csv/                    # CSVs GFW (excluidos del repo por .gitignore)
│   ├── external/               # Datasets externos (excluidos del repo)
│   │   ├── worldbank/
│   │   ├── faostat/
│   │   └── geonames/
│   └── results/                # Figuras generadas por los notebooks
├── docs/
│   ├── architecture.drawio     # Diagrama de arquitectura (editable)
│   ├── setup.md                # Instrucciones detalladas de configuración
│   └── datasets_externos.md    # Documentación de fuentes externas
├── notebooks/
│   ├── 01_eda_gfw.ipynb        # EDA del dataset GFW raw
│   ├── 02_eda_worldbank.ipynb  # EDA indicadores World Bank
│   ├── 03_eda_faostat.ipynb    # EDA producción FAO
│   ├── 04_eda_geonames.ipynb   # EDA localidades GeoNames
│   ├── 05_etl_pipeline.ipynb   # ETL unificado + migración + visualizaciones
│   └── etl_eda.ipynb           # Notebook original (primera entrega)
├── scripts/
│   ├── batch_download.py       # Orquestador: ejecuta todos los downloads + S3
│   ├── sync_alerts.py          # Descarga incremental de alertas GFW
│   ├── download_dataset.py     # Descarga inicial completa GFW (primera entrega)
│   ├── download_worldbank.py   # Descarga indicadores World Bank API
│   ├── download_faostat.py     # Descarga producción agrícola FAO (bulk CSV)
│   ├── download_geonames.py    # Descarga localidades pobladas GeoNames
│   ├── upload_to_s3.py         # Sube CSVs al bucket S3
│   ├── fetch_from_proxy.py     # Descarga CSVs desde proxy inverso (S3 → local)
│   └── gfw_signup.py           # Registro y obtención de API key GFW
├── .env.example                # Plantilla de variables de entorno
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Scripts — qué hace cada uno

### `batch_download.py`
Orquestador principal. Llama en secuencia a todos los scripts de descarga y luego sube a S3. Diseñado para correr como tarea programada (cron) en el servidor Lightsail.

```bash
python scripts/batch_download.py                  # descarga todo + sube a S3
python scripts/batch_download.py --skip-gfw       # omite GFW (solo externos)
python scripts/batch_download.py --skip-upload    # no sube a S3
python scripts/batch_download.py --full-gfw       # GFW completo (no solo --daily)
```

### `sync_alerts.py`
Descarga incremental de alertas GFW. Detecta automáticamente la última fecha en el CSV existente y descarga solo los días nuevos. Usa ventanas adaptativas por densidad de alertas.

```bash
python scripts/sync_alerts.py                     # desde última fecha hasta ayer
python scripts/sync_alerts.py --daily             # solo el día de ayer (modo cron)
python scripts/sync_alerts.py --from 2026-01-01   # fuerza inicio desde esa fecha
python scripts/sync_alerts.py --country BOL       # solo un país
```

### `download_dataset.py`
Descarga inicial completa del dataset GFW (primera entrega). Genera los CSVs históricos completos para BOL y COL. Solo necesario la primera vez.

```bash
python scripts/download_dataset.py   # menú interactivo
```

### `download_worldbank.py`
Descarga 4 indicadores del World Bank API para Bolivia y Colombia (2015–2024): PIB, población rural, tierra agrícola y exportaciones agrícolas.

```bash
python scripts/download_worldbank.py
# Salida: data/external/worldbank/worldbank_indicators.csv
```

### `download_faostat.py`
Descarga el bulk CSV de producción agrícola de FAOSTAT para Américas y filtra Bolivia y Colombia. Productos: Soybeans, Cattle, Sugar cane, Oil palm fruit.

```bash
python scripts/download_faostat.py
# Salida: data/external/faostat/faostat_production.csv
```

### `download_geonames.py`
Descarga los dumps ZIP de GeoNames para Bolivia (BO.zip) y Colombia (CO.zip) y extrae todas las localidades pobladas (~61k lugares con lat/lon).

```bash
python scripts/download_geonames.py
# Salida: data/external/geonames/geonames_places.csv
```

### `upload_to_s3.py`
Sube todos los CSVs de `data/csv/` y `data/external/` al bucket S3 configurado en `.env`. Mantiene la estructura de carpetas.

```bash
python scripts/upload_to_s3.py             # sube todo
python scripts/upload_to_s3.py --dry-run   # muestra qué subiría sin subir
```

### `fetch_from_proxy.py`
Descarga los CSVs desde el proxy inverso (que apunta al bucket S3) al entorno local. El notebook lo llama antes de leer los datos.

```bash
python scripts/fetch_from_proxy.py              # descarga todo
python scripts/fetch_from_proxy.py --only gfw   # solo alertas GFW
python scripts/fetch_from_proxy.py --only external  # solo datasets externos
```

### `gfw_signup.py`
Registra un email en la API de Global Forest Watch y obtiene la API key. Solo necesario la primera vez.

```bash
python scripts/gfw_signup.py
```

---

## Notebooks — orden de ejecución

| Orden | Notebook | Propósito |
|-------|----------|-----------|
| 1 | `01_eda_gfw.ipynb` | EDA del CSV raw GFW. Analiza estructura, nulos, distribuciones temporales y espaciales. Documenta las decisiones de transformación. |
| 2 | `02_eda_worldbank.ipynb` | EDA de indicadores económicos. Tendencias de PIB, tierra agrícola y población rural 2015–2024. |
| 3 | `03_eda_faostat.ipynb` | EDA de producción agrícola. Evolución de soya, ganadería y palma de aceite por país. |
| 4 | `04_eda_geonames.ipynb` | EDA de localidades pobladas. Distribución geográfica y estadísticas de población. |
| 5 | `05_etl_pipeline.ipynb` | **Pipeline completo:** carga las 4 fuentes → transforma → merge → migra al star schema → carga incremental → EDA desde DB → visualizaciones integradas. |

> `etl_eda.ipynb` es el notebook original de la primera entrega. No se modifica.

---

## Instalación y configuración

### 1. Clonar el repositorio

```bash
git clone https://github.com/<tu-usuario>/deforestation-alert-etl.git
cd deforestation-alert-etl
```

### 2. Crear entorno virtual con Python 3.12

```bash
python3.12 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

> **Nota macOS (Homebrew):** si `pip` falla con `externally-managed-environment`, asegúrate de estar dentro del venv (`which python` debe apuntar a `venv/bin/python`).

### 4. Configurar variables de entorno

```bash
cp .env.example .env
# Edita .env con tus credenciales
```

Variables requeridas:

| Variable | Descripción |
|----------|-------------|
| `GFW_API_KEY` | API key de Global Forest Watch |
| `PG_USER` / `PG_PASSWORD` / `PG_HOST` / `PG_PORT` / `PG_DB` | Conexión PostgreSQL |
| `S3_BUCKET` | Nombre del bucket S3 |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` | Credenciales AWS |
| `DATA_PROXY_URL` | URL base del proxy inverso que expone los CSVs desde S3 |

### 5. Crear la base de datos PostgreSQL

```bash
# macOS
brew install postgresql@16
brew services start postgresql@16
psql -U postgres -c "CREATE DATABASE deforestation;"
```

### 6. Obtener API key de GFW (primera vez)

```bash
python scripts/gfw_signup.py
# Copia la key generada en .env → GFW_API_KEY
```

---

## Cómo ejecutar el pipeline

### Opción A — Ejecución local completa (primera vez)

```bash
# 1. Descargar dataset GFW histórico (~95 MB, puede tardar horas)
python scripts/download_dataset.py

# 2. Descargar datasets externos
python scripts/download_worldbank.py
python scripts/download_faostat.py
python scripts/download_geonames.py

# 3. Subir a S3
python scripts/upload_to_s3.py

# 4. Abrir Jupyter y ejecutar notebooks en orden
jupyter notebook
# Ejecutar: 01 → 02 → 03 → 04 → 05
```

### Opción B — Ejecución local con datos desde S3 (uso diario)

```bash
# 1. Descargar CSVs actualizados desde el proxy
python scripts/fetch_from_proxy.py

# 2. Ejecutar solo el pipeline ETL
jupyter notebook notebooks/05_etl_pipeline.ipynb
# La sección 5 del notebook detecta automáticamente los datos nuevos
# y solo inserta las filas que no están en la DB
```

### Opción C — Batch en Lightsail (automático, cron diario)

El servidor Lightsail ejecuta automáticamente a las 06:00 UTC:

```bash
# Equivalente a lo que corre el cron:
python scripts/batch_download.py --daily
```

Para forzar manualmente desde GitHub: ir a **Actions → Daily Batch Download → Run workflow**.

---

## Despliegue en AWS Lightsail

### Configuración inicial del servidor

```bash
# 1. Conectar al servidor
ssh ubuntu@<LIGHTSAIL_IP>

# 2. Instalar dependencias del sistema
sudo apt update && sudo apt install -y python3.12 python3.12-venv git

# 3. Clonar el repositorio
git clone https://github.com/<tu-usuario>/deforestation-alert-etl.git
cd deforestation-alert-etl

# 4. Crear venv e instalar dependencias
python3.12 -m venv venv
venv/bin/pip install -r requirements.txt

# 5. Configurar .env con las credenciales
cp .env.example .env
nano .env   # completar con valores reales

# 6. Crear directorio de logs
mkdir -p ~/logs
```

### Configurar cron manualmente (alternativa a GitHub Actions)

```bash
crontab -e
# Agregar:
0 6 * * * /home/ubuntu/deforestation-alert-etl/venv/bin/python /home/ubuntu/deforestation-alert-etl/scripts/batch_download.py >> /home/ubuntu/logs/batch.log 2>&1
```

### Secrets requeridos en GitHub

Ir a **Settings → Secrets and variables → Actions** y agregar:

| Secret | Valor |
|--------|-------|
| `LIGHTSAIL_HOST` | IP pública del servidor Lightsail |
| `LIGHTSAIL_USER` | Usuario SSH (normalmente `ubuntu`) |
| `LIGHTSAIL_SSH_KEY` | Contenido completo de la clave privada SSH (`.pem`) |
| `GFW_API_KEY` | API key de Global Forest Watch |
| `PG_USER` / `PG_PASSWORD` / `PG_HOST` / `PG_PORT` / `PG_DB` | PostgreSQL |
| `S3_BUCKET` / `S3_PREFIX` | Bucket y prefijo S3 |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` | AWS |
| `DATA_PROXY_URL` | URL del proxy inverso |

---

## GitHub Actions CI/CD

### `deploy.yml` — Deploy automático

Se activa en cada push a `main` que modifique archivos en `scripts/` o `requirements.txt`.

**Pasos:**
1. Conecta al servidor Lightsail por SSH
2. Hace `git pull origin main`
3. Actualiza el venv con `pip install -r requirements.txt`
4. Escribe el archivo `.env` desde los secrets de GitHub
5. Instala/actualiza el cron job

### `batch.yml` — Ejecución diaria programada

Se activa automáticamente a las 06:00 UTC o manualmente desde la UI de GitHub Actions.

**Parámetros opcionales (workflow_dispatch):**
- `skip_gfw`: omite la descarga de alertas GFW
- `skip_upload`: omite la subida a S3

---

## Modelo de datos (Star Schema)

```
                    dim_date
                    ┌──────────────┐
                    │ date_id (PK) │
                    │ date         │
                    │ year · month │
                    │ week         │
                    └──────┬───────┘
                           │
dim_location        fact_alerts          dim_driver
┌─────────────┐    ┌──────────────────┐  ┌─────────────┐
│ location_id │◄───│ alert_id (PK)    │  │ driver_id   │
│ country_code│    │ date_id (FK)     │──►│ driver_name │
│ country_name│    │ location_id (FK) │  └─────────────┘
│ adm1_code   │    │ driver_id (FK)   │
│ adm1_name   │    │ land_cover_id(FK)│  dim_land_cover
└─────────────┘    │ confidence_id(FK)│  ┌──────────────────┐
                   │ econ_id (FK) ★   │  │ land_cover_id    │
dim_confidence     │ lat · lon        │  │ land_cover_class │
┌──────────────┐   │ tree_cover_pct   │  └──────────────────┘
│confidence_id │◄──│ is_primary_forest│
│ confidence   │   │ protected_area   │  dim_economic_context ★
│ is_high_conf │   │ is_soy_area      │  ┌──────────────────────┐
└──────────────┘   │ dist_place_km ★  │  │ econ_id              │
                   └──────────────────┘  │ country_code · year  │
                                         │ gdp_usd              │
dim_places ★                             │ agricultural_land_pct│
┌──────────────────┐                     │ soy_production_t     │
│ place_id         │                     │ soy_area_ha          │
│ place_name       │                     └──────────────────────┘
│ lat · lon        │
│ population       │
└──────────────────┘

★ = Nuevas tablas/columnas en la segunda entrega
```

---

## Alineación con los ODS

| ODS | Contribución |
|-----|-------------|
| **ODS 13 — Acción por el clima** | Las alertas de deforestación son un indicador directo de emisiones de CO₂. El cruce con datos de producción agrícola (FAO) permite estimar la presión económica sobre los bosques. |
| **ODS 15 — Vida de ecosistemas terrestres** | El dataset registra pérdida de bosque primario, categorías de áreas protegidas y drivers de deforestación. La distancia a centros poblados (GeoNames) cuantifica la presión humana. |

---

## Licencia

Proyecto académico — Ingeniería de Datos e IA, 2026.  
Dataset GFW bajo licencia [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/).  
Datos World Bank y FAO bajo licencias abiertas de sus respectivas organizaciones.
