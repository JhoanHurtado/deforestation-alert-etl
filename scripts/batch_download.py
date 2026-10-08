#!/usr/bin/env python3
"""
batch_download.py — Orquesta la descarga de todos los datasets y sube a S3.

Flujo:
  1. sync_alerts.py --daily          (GFW — alertas incrementales)
  2. download_worldbank.py           (World Bank — indicadores económicos)
  3. download_faostat.py             (FAO — producción agrícola)
  4. download_geonames.py            (GeoNames — localidades pobladas)
  5. scrape_protected_areas.py       (Wikipedia/WDPA — áreas protegidas BOL+COL)
  6. validate_quality.py             (Great Expectations — control de calidad)
  7. upload_to_s3.py                 (sube CSVs a S3 y elimina copias locales)

Gestión de logs:
  - Registra cada paso en logs/batch_YYYY-MM-DD.log y consola.
  - Sincroniza el log resultante a s3://<bucket>/deforestacion-alert-etl/logs/
  - Si alguna descarga o subida falla, queda registrado el traceback y código de error.

Programación diaria (22:00 / 10:00 PM):
  0 22 * * * /home/ubuntu/venv/bin/python /home/ubuntu/deforestation-alert-etl/scripts/batch_download.py >> /home/ubuntu/logs/batch.log 2>&1
"""

import os
import sys
import logging
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

SCRIPTS_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPTS_DIR.parent
LOG_DIR     = PROJECT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

DATE_STR = datetime.now().strftime("%Y-%m-%d")
LOG_FILE = LOG_DIR / f"batch_{DATE_STR}.log"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("batch_download")

# Python interpreter del venv
_venv_python = PROJECT_DIR / "venv" / "bin" / "python"
PYTHON = _venv_python if _venv_python.exists() else Path(sys.executable).resolve()

BUCKET = os.getenv("S3_BUCKET")
PREFIX = os.getenv("S3_PREFIX", "deforestacion-alert-etl/").rstrip("/") + "/"
REGION = os.getenv("AWS_REGION", "us-east-1")


def sync_log_to_s3():
    """Sube el archivo de log a S3 para trazabilidad remota."""
    if not BUCKET:
        return
    try:
        import boto3
        s3 = boto3.client("s3", region_name=REGION)
        ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        log_key = f"{PREFIX}logs/batch_{ts}.log"
        s3.upload_file(str(LOG_FILE), BUCKET, log_key)
        logger.info(f"📋 Log de ejecución sincronizado a s3://{BUCKET}/{log_key}")
    except Exception as e:
        logger.warning(f"No se pudo sincronizar el log a S3: {e}")


def run(script: str, extra_args: list[str] = []) -> bool:
    path = SCRIPTS_DIR / script
    cmd  = [str(PYTHON), str(path)] + extra_args
    logger.info(f"{'='*55}")
    logger.info(f"▶ Ejecutando: {script} {' '.join(extra_args)}")
    logger.info(f"{'='*55}")

    result = subprocess.run(cmd, cwd=PROJECT_DIR, capture_output=False)
    if result.returncode != 0:
        logger.error(f"❌ {script} falló con código de salida {result.returncode}")
        return False
    logger.info(f"✅ {script} completado exitosamente.")
    return True


def main():
    parser = argparse.ArgumentParser(description="Batch download y sincronización a S3 con rotación y logs")
    parser.add_argument("--skip-gfw",     action="store_true", help="Omite sync_alerts.py")
    parser.add_argument("--skip-quality", action="store_true", help="Omite validate_quality.py")
    parser.add_argument("--skip-upload",  action="store_true", help="Omite upload_to_s3.py")
    parser.add_argument("--keep-local",   action="store_true", help="Conserva CSVs locales sin eliminarlos")
    parser.add_argument("--full-gfw",     action="store_true", help="Descarga GFW histórico completo (no solo --daily)")
    args = parser.parse_args()

    start = datetime.now()
    logger.info(f"🚀 INICIO BATCH PIPELINE — {start.strftime('%Y-%m-%d %H:%M:%S')}")

    results = {}

    if not args.skip_gfw:
        gfw_args = [] if args.full_gfw else ["--daily"]
        results["gfw"] = run("sync_alerts.py", gfw_args)

    results["worldbank"]       = run("download_worldbank.py")
    results["faostat"]         = run("download_faostat.py")
    results["geonames"]        = run("download_geonames.py")
    results["protected_areas"] = run("scrape_protected_areas.py")

    if not args.skip_quality:
        results["data_quality"] = run("validate_quality.py")

    if not args.skip_upload:
        up_args = ["--keep-local"] if args.keep_local else []
        results["s3_upload"] = run("upload_to_s3.py", up_args)

    elapsed = (datetime.now() - start).seconds
    logger.info(f"{'='*55}")
    logger.info(f"🏁 RESUMEN DEL BATCH ({elapsed} segundos):")
    for name, ok in results.items():
        status = "✅ PASÓ" if ok else "❌ FALLÓ"
        logger.info(f"   {status:8} | {name}")
    logger.info(f"{'='*55}")

    # Sincronizar archivo de log a S3
    sync_log_to_s3()

    if not all(results.values()):
        logger.error("El pipeline finalizó con uno o más errores.")
        sys.exit(1)

    logger.info("Pipeline completado exitosamente sin errores.")


if __name__ == "__main__":
    main()
