# Setup

## 1. Entorno Python

```bash
# Crear entorno virtual
python3.12 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

pip install pandas sqlalchemy psycopg2-binary jupyter requests \
            matplotlib seaborn python-dotenv
```

## 2. PostgreSQL

### Instalar (macOS)
```bash
brew install postgresql@16
brew services start postgresql@16
```

### Crear base de datos
```bash
psql -U postgres -c "CREATE DATABASE deforestation;"
```

### Configurar credenciales
Edita `.env` con tus valores:
```
PG_USER=postgres
PG_PASSWORD=postgres
PG_HOST=localhost
PG_PORT=5432
PG_DB=deforestation
```

## 3. Descargar dataset

```bash
# Solo la primera vez — obtener API key de GFW
python scripts/gfw_signup.py

# Descargar alertas BOL y COL (puede tardar varios minutos)
python scripts/download_dataset.py
```

Los CSV se guardan en `data/csv/`.

## 4. Ejecutar el notebook

```bash
jupyter notebook notebooks/etl_eda.ipynb
```

Ejecutar las celdas en orden:
1. Imports y configuración
2. Carga de CSVs
3. Calidad de datos
4. Transformación
5. **Migración a PostgreSQL** ← requiere DB creada
6. EDA desde la DB
7. Visualizaciones
8. Resumen ejecutivo
