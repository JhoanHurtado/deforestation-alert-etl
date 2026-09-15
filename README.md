# 🌳 Deforestation Alert ETL

Pipeline ETL para el análisis de alertas de deforestación en Bolivia y Colombia (2022–2026), construido como proyecto académico para el curso de **ETL** del programa de Ingeniería de Datos e Inteligencia Artificial.

Alineado con los **ODS 13 — Acción por el clima** y **ODS 15 — Vida de ecosistemas terrestres** de la ONU.

---

## Tabla de contenidos

- [Descripción del dataset](#descripción-del-dataset)
- [Objetivos de análisis](#objetivos-de-análisis)
- [Modelo de datos](#modelo-de-datos-star-schema)
- [Stack tecnológico](#stack-tecnológico)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Instalación y uso](#instalación-y-uso)
- [Documentación](#documentación)
- [Alineación con los ODS](#alineación-con-los-ods)

---

## Descripción del dataset

**Fuente:** [Global Forest Watch (GFW)](https://data-api.globalforestwatch.org) — `gfw_integrated_alerts`  
**Cobertura geográfica:** Bolivia (BOL) y Colombia (COL)  
**Rango temporal:** 2022-01-01 → 2026-07-30  
**Volumen total:** ~1,523,990 filas

| País | Filas |
|------|-------|
| Bolivia (BOL) | 724,000 |
| Colombia (COL) | 799,990 |

Cada fila representa **un píxel satelital con alerta de deforestación**. Las 12 columnas originales cubren:

| Columna | Descripción |
|---------|-------------|
| `latitude` / `longitude` | Coordenadas geográficas del píxel |
| `gfw_integrated_alerts__date` | Fecha de detección de la alerta |
| `gfw_integrated_alerts__confidence` | Nivel de confianza (`high` / `highest` / `nominal`) |
| `umd_tree_cover_density_2000__percent` | Densidad de cobertura arbórea en 2000 (0–100%) |
| `is__umd_regional_primary_forest_2001` | 1 si el píxel era bosque primario en 2001 |
| `wdpa_protected_areas__iucn_cat` | Categoría IUCN del área protegida (0 = no protegida) |
| `esa_land_cover_2015__class` | Clase de uso del suelo en 2015 |
| `wri_google_tree_cover_loss_drivers__category` | Código del driver principal de pérdida forestal |
| `gadm_administrative_boundaries__adm1` | ID del departamento/región administrativa |
| `is__umd_soy_planted_area_buffered_10km` | 1 si está dentro de 10 km de área de soya |

---

## Objetivos de análisis

### Análisis espaciales y geográficos
- **Identificación de patrones:** localizar dónde se concentra la pérdida forestal a nivel de departamento y coordenada.
- **Proximidad e influencia:** medir la distancia de la deforestación a carreteras, ríos, centros poblados y áreas protegidas.
- **Fragmentación de hábitats:** evaluar cómo los parches de bosque restante afectan la conectividad ecológica y la biodiversidad local.

### Análisis temporales y de tendencias
- **Tasas de cambio:** calcular la velocidad y aceleración de la pérdida de bosques por año, mes y estación climática.
- **Estacionalidad:** identificar los meses de mayor actividad de deforestación por país (ej. temporada seca Bolivia: julio–noviembre).

### Estudios ambientales y socioeconómicos (analisis tentativos)
- **Emisiones de carbono:** estimar el CO₂ liberado a la atmósfera por quema y remoción de biomasa.
- **Causalidad:** cruzar los datos forestales con variables económicas, agrícolas o demográficas para comprender los principales drivers de deforestación (agricultura, ganadería, minería, infraestructura).
- **Impacto en cuencas hidrográficas:** evaluar los efectos de la pérdida de vegetación sobre el ciclo del agua y la erosión del suelo.
- **Bosque primario vs. secundario:** comparar la proporción de pérdida en bosques primarios (irreemplazables) frente a secundarios.

## Modelo de datos (Star Schema)

La tabla de hechos `fact_alerts` centraliza cada alerta con claves foráneas a cinco dimensiones: fecha, ubicación geográfica, driver de pérdida, tipo de cobertura de suelo y nivel de confianza.

---

## Stack tecnológico

| Capa | Herramienta | Justificación |
|------|-------------|---------------|
| Extracción | Python + `requests` | Consumo de API REST con reintentos y ventanas adaptativas |
| Almacenamiento raw | CSV por país | Checkpoint de recuperación ante fallos de la API |
| Base de datos | PostgreSQL | RDBMS con soporte completo de JOINs, índices y tipos geoespaciales |
| Transformación | `pandas` | Renombrado, derivación de columnas temporales, resolución de FKs |
| Migración | `SQLAlchemy` + `psycopg2` | ORM + driver nativo para carga eficiente en bulk |
| Visualización | `matplotlib` + `seaborn` | Gráficos estadísticos reproducibles en Jupyter |
| Orquestación | Jupyter Notebook | Reproducibilidad y documentación integrada en un solo artefacto |

---

## Estructura del proyecto

```
deforestation-alert-etl/
├── data/
│   ├── csv/                    # CSVs raw descargados desde GFW
│   │   ├── bol_alerts_*.csv
│   │   ├── col_alerts_*.csv
│   │   ├── failed_windows_*.json
│   │   └── preview_*.json
│   └── results/                # Figuras generadas por el notebook
│       ├── fig1_alertas_anuales_por_pais.png
│       ├── fig2_estacionalidad_por_pais.png
│       ├── fig3_drivers_bolivia.png
│       ├── fig4_drivers_colombia.png
│       ├── fig5_bosque_primario_vs_secundario.png
│       ├── fig6_densidad_cobertura.png
│       └── fig7_alertas_por_departamento.png
├── docs/
│   ├── architecture.drawio          # Diagrama de arquitectura editable
│   ├── diagrama.png                 # Diagrama exportado
│   ├── setup.md                     # Instrucciones de configuración del entorno
├── notebooks/
│   └── etl_eda.ipynb           # Notebook principal: ETL + EDA + visualizaciones
├── scripts/
│   ├── download_dataset.py     # Descarga inicial desde GFW API
│   ├── gfw_signup.py           # Registro y obtención de API key
│   └── sync_alerts.py          # Sincronización incremental de nuevas alertas
├── .env.example                # Plantilla de variables de entorno
├── .gitignore
└── README.md
```

---

## Instalación y uso

### 1. Clonar el repositorio

```bash
git clone https://github.com/<tu-usuario>/deforestation-alert-etl.git
cd deforestation-alert-etl
```

### 2. Crear entorno virtual e instalar dependencias

```bash
python3.12 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

pip install pandas sqlalchemy psycopg2-binary jupyter requests \
            matplotlib seaborn python-dotenv
```

### 3. Configurar variables de entorno

```bash
cp .env.example .env
# Edita .env con tus credenciales de PostgreSQL y la API key de GFW
```

### 4. Crear la base de datos PostgreSQL

```bash
brew install postgresql@16     # macOS
brew services start postgresql@16
psql -U postgres -c "CREATE DATABASE deforestation;"
```

### 5. Descargar el dataset

```bash
# Obtener API key de GFW (solo la primera vez)
python scripts/gfw_signup.py

# Descargar alertas BOL y COL (~95 MB, puede tardar varias horas o dias)
python scripts/download_dataset.py
```

> Para instrucciones detalladas de configuración ver [docs/setup.md](docs/setup.md).

### 6. Ejecutar el notebook

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

### Sincronización incremental (opcional)

```bash
python scripts/sync_alerts.py              # desde última fecha hasta ayer
python scripts/sync_alerts.py --daily      # solo el día de ayer (modo cron)
python scripts/sync_alerts.py --country BOL  # solo un país
```

---

## Alineación con los ODS

| ODS | Contribución |
|-----|-------------|
| **ODS 13 — Acción por el clima** | Las alertas de deforestación son un indicador directo de emisiones de CO₂ por pérdida de cobertura forestal |
| **ODS 15 — Vida de ecosistemas terrestres** | El dataset registra pérdida de bosque primario, categorías de uso del suelo y presencia en áreas protegidas |

---

## Licencia
Proyecto académico — Ingeniería de Datos e IA, 2026.  
Dataset provisto por [Global Forest Watch](https://www.globalforestwatch.org/) bajo licencia Creative Commons Attribution 4.0.
