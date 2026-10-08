#!/usr/bin/env bash
# ==============================================================================
# start_airflow_local.sh — Inicializa y lanza Apache Airflow en tu máquina local
# ==============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export AIRFLOW_HOME="$SCRIPT_DIR/airflow_local"
export AIRFLOW__CORE__DAGS_FOLDER="$SCRIPT_DIR/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT="9179"
export PROJECT_DIR="$SCRIPT_DIR"

# Activar venv si existe
if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
    source "$SCRIPT_DIR/venv/bin/activate"
fi

echo "=================================================="
echo "🚀 Configurando Apache Airflow Local en: $AIRFLOW_HOME"
echo "📂 DAGs folder: $AIRFLOW__CORE__DAGS_FOLDER"
echo "🔌 Puerto Web: http://localhost:9179"
echo "=================================================="

mkdir -p "$AIRFLOW_HOME"

# Configurar contraseña para SimpleAuthManager y Standalone
echo '{"admin": "admin"}' > "$AIRFLOW_HOME/simple_auth_manager_passwords.json"
echo "admin" > "$AIRFLOW_HOME/standalone_admin_password.txt"

# Inicializar DB si no existe
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    echo "── Inicializando base de datos local SQLite ──"
    airflow db migrate 2>/dev/null || airflow db init 2>/dev/null || true

    echo "── Creando usuario admin (admin / admin) ──"
    airflow users create \
        --username admin \
        --firstname Jhoan \
        --lastname Hurtado \
        --role Admin \
        --email jhoanezequielh@gmail.com \
        --password admin 2>/dev/null || true
fi

# Despausar el DAG para que esté visible y listo
echo "── Registrando y activando deforestation_etl_dag ──"
airflow dags list 2>/dev/null || true
airflow dags unpause deforestation_etl_dag 2>/dev/null || true

echo ""
echo "✅ Airflow local listo."
echo "Abriendo Airflow Standalone (Webserver + Scheduler)..."
echo "👉 Abre tu navegador en: http://localhost:9179"
echo "🔑 Usuario: admin  |  Contraseña: admin"
echo "Pulsa Ctrl + C para detener."
echo "=================================================="

exec airflow standalone

