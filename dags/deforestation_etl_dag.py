"""
deforestation_etl_dag.py — DAG Airflow para el pipeline de alertas de deforestación.

Schedule: diario 06:00 UTC
Orden:
  1. sync_gfw_alerts                          (fuente principal, secuencial)
  2. download_worldbank  ┐
   | download_faostat    ├── paralelo
   | download_geonames   │
   | scrape_protected_areas ┘
  3. upload_to_s3                             (espera a todos)
  4. run_etl_pipeline                         (notebook ejecutado con nbconvert)
"""

from datetime import datetime, timedelta
from airflow import DAG
try:
    from airflow.operators.bash import BashOperator
except ImportError:
    from airflow.providers.standard.operators.bash import BashOperator


PROJECT = "/home/ubuntu/deforestation-alert-etl"
PYTHON  = f"{PROJECT}/venv/bin/python"

default_args = {
    "owner": "etl-g51",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="deforestation_etl_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 22 * * *",
    catchup=False,
    tags=["deforestation", "etl", "ods13", "ods15", "great_expectations"],
) as dag:

    sync_gfw = BashOperator(
        task_id="sync_gfw_alerts",
        bash_command=f"{PYTHON} {PROJECT}/scripts/sync_alerts.py --daily",
    )

    dl_worldbank = BashOperator(
        task_id="download_worldbank",
        bash_command=f"{PYTHON} {PROJECT}/scripts/download_worldbank.py",
    )

    dl_faostat = BashOperator(
        task_id="download_faostat",
        bash_command=f"{PYTHON} {PROJECT}/scripts/download_faostat.py",
    )

    dl_geonames = BashOperator(
        task_id="download_geonames",
        bash_command=f"{PYTHON} {PROJECT}/scripts/download_geonames.py",
    )

    scrape_areas = BashOperator(
        task_id="scrape_protected_areas",
        bash_command=f"{PYTHON} {PROJECT}/scripts/scrape_protected_areas.py",
    )

    upload_s3 = BashOperator(
        task_id="upload_to_s3",
        bash_command=f"{PYTHON} {PROJECT}/scripts/upload_to_s3.py",
    )

    validate_quality = BashOperator(
        task_id="validate_data_quality",
        bash_command=f"{PYTHON} {PROJECT}/scripts/validate_quality.py",
    )

    run_etl = BashOperator(
        task_id="run_etl_pipeline",
        bash_command=(
            f"cd {PROJECT} && {PROJECT}/venv/bin/jupyter nbconvert "
            f"--to notebook --execute notebooks/05_etl_pipeline.ipynb "
            f"--output notebooks/05_etl_pipeline_executed.ipynb"
        ),
        execution_timeout=timedelta(hours=2),
    )

    sync_gfw >> [dl_worldbank, dl_faostat, dl_geonames, scrape_areas]
    [dl_worldbank, dl_faostat, dl_geonames, scrape_areas] >> validate_quality
    validate_quality >> run_etl >> upload_s3
