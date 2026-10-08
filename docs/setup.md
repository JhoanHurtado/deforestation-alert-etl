# Setup — Segunda Entrega

## 1. Entorno Python

```bash
# Crear entorno virtual con Python 3.12
python3.12 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

## 2. PostgreSQL

### Instalar (macOS)
```bash
brew install postgresql@16
brew services start postgresql@16
```

### Instalar (Ubuntu / Lightsail)
```bash
sudo apt update && sudo apt install -y postgresql
sudo systemctl start postgresql
```

### Instalar (Windows)
Descargar desde https://www.postgresql.org/download/windows/ e instalar con el instalador gráfico.

### Crear base de datos
```bash
psql -U postgres -c "CREATE DATABASE deforestation;"
```

## 3. Configurar variables de entorno

```bash
cp .env.example .env
# Editar .env con los valores reales
```

Variables requeridas:

| Variable | Descripción |
|----------|-------------|
| `GFW_API_KEY` | API key de Global Forest Watch |
| `PG_USER` | Usuario de PostgreSQL |
| `PG_PASSWORD` | Contraseña de PostgreSQL |
| `PG_HOST` | Host de PostgreSQL (normalmente `localhost`) |
| `PG_PORT` | Puerto de PostgreSQL (normalmente `5432`) |
| `PG_DB` | Nombre de la base de datos (p.ej. `deforestation`) |
| `S3_BUCKET` | Nombre del bucket S3 |
| `S3_PREFIX` | Prefijo de carpeta en S3 (opcional) |
| `AWS_ACCESS_KEY_ID` | Credencial AWS |
| `AWS_SECRET_ACCESS_KEY` | Credencial AWS |
| `AWS_REGION` | Región AWS (p.ej. `us-east-1`) |
| `DATA_PROXY_URL` | URL base del proxy inverso que expone los CSVs desde S3 |

## 4. Descargar los datos

### Opción A — Automática (requiere DATA_PROXY_URL configurado)

Con `DATA_PROXY_URL` configurado en `.env`, los notebooks descargarán los datos automáticamente al ejecutarse. No se necesita ningún paso adicional.

### Opción B — Script de descarga manual

```bash
# Descargar todos los CSVs desde el proxy inverso
python scripts/fetch_from_proxy.py

# O descargar directamente desde las fuentes originales:
python scripts/download_dataset.py           # Alertas GFW (primera vez)
python scripts/sync_alerts.py --daily        # Alertas GFW (incremental)
python scripts/download_worldbank.py         # Indicadores World Bank
python scripts/download_faostat.py           # Producción agrícola FAO
python scripts/download_geonames.py          # Localidades pobladas GeoNames
```

Los CSVs se guardan en:
- `data/csv/` — alertas GFW
- `data/external/worldbank/` — indicadores económicos
- `data/external/faostat/` — producción agrícola
- `data/external/geonames/` — localidades pobladas

## 5. Ejecutar los notebooks en orden

```bash
jupyter notebook
```

Ejecutar en este orden:

| Orden | Notebook | Propósito |
|-------|----------|-----------|
| 1 | `01_eda_gfw.ipynb` | EDA de alertas GFW. Análisis temporal, espacial, causalidad y proximidad a poblados. |
| 2 | `02_eda_worldbank.ipynb` | EDA indicadores económicos. Tendencias y correlaciones. |
| 3 | `03_eda_faostat.ipynb` | EDA producción agrícola. Composición y variación YoY. |
| 4 | `04_eda_geonames.ipynb` | EDA localidades pobladas. Distribución geográfica y densidad. |
| 5 | `05_etl_pipeline.ipynb` | Pipeline ETL completo: transforma, integra y carga en PostgreSQL. |

## 6. Obtener API key de GFW (solo la primera vez)

```bash
python scripts/gfw_signup.py
# Copiar la key generada en .env → GFW_API_KEY
```
