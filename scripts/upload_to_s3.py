#!/usr/bin/env python3
"""
upload_to_s3.py — Sube los CSVs descargados al bucket S3 y elimina copias locales.

Sube con la estructura:
  data/csv/          → s3://<bucket>/<prefix>gfw/
  data/external/     → s3://<bucket>/<prefix>external/

Comportamiento:
  1. Sube cada archivo a S3 bajo el prefijo configurado (default: deforestacion-alert-etl/).
  2. Tras confirmar la subida exitosa de cada CSV, lo elimina automáticamente localmente.
  3. Registra logs detallados en logs/s3_upload.log y los sincroniza a S3 (logs/).

Uso:
  python scripts/upload_to_s3.py
  python scripts/upload_to_s3.py --keep-local   # sube sin eliminar archivos locales
  python scripts/upload_to_s3.py --dry-run      # muestra qué subiría sin modificar nada
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

PROJECT_DIR = Path(__file__).parent.parent
LOG_DIR = PROJECT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "s3_upload.log"

# ── Logger setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("upload_to_s3")

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    logger.error("boto3 no está instalado. Ejecuta: pip install boto3")
    sys.exit(1)

BUCKET = os.getenv("S3_BUCKET")
PREFIX = os.getenv("S3_PREFIX", "deforestacion-alert-etl/").rstrip("/") + "/"
REGION = os.getenv("AWS_REGION", "us-east-1")

DATA_ROOT = PROJECT_DIR / "data"

# Mapeo local → prefijo S3
UPLOAD_DIRS = {
    DATA_ROOT / "csv":      f"{PREFIX}gfw/",
    DATA_ROOT / "external": f"{PREFIX}external/",
}

EXTENSIONS = {".csv", ".json"}


def upload(dry_run: bool, keep_local: bool):
    if not BUCKET:
        logger.error("S3_BUCKET no configurado en el archivo .env")
        sys.exit(1)

    try:
        s3 = boto3.client("s3", region_name=REGION)
    except Exception as e:
        logger.error(f"Error conectando a AWS S3: {e}")
        sys.exit(1)

    total_uploaded = 0
    total_deleted = 0
    errors = 0

    logger.info(f"Iniciando subida a s3://{BUCKET}/{PREFIX} (dry_run={dry_run}, keep_local={keep_local})")

    for local_dir, s3_prefix in UPLOAD_DIRS.items():
        if not local_dir.exists():
            logger.info(f"Directorio local {local_dir} no existe — omitido")
            continue

        for file in sorted(local_dir.rglob("*")):
            if file.suffix not in EXTENSIONS or not file.is_file():
                continue

            relative = file.relative_to(local_dir)
            s3_key   = s3_prefix + str(relative).replace("\\", "/")
            size_mb  = file.stat().st_size / 1_048_576

            logger.info(f"{'[DRY RUN] ' if dry_run else ''}Subiendo: {file.name} ({size_mb:.2f} MB) → s3://{BUCKET}/{s3_key}")

            if not dry_run:
                try:
                    s3.upload_file(str(file), BUCKET, s3_key)
                    total_uploaded += 1
                    logger.info(f"  ✅ Subido exitosamente: s3://{BUCKET}/{s3_key}")

                    # Eliminar automáticamente tras confirmación si no se especificó keep_local
                    if not keep_local:
                        try:
                            file.unlink()
                            total_deleted += 1
                            logger.info(f"  🗑️ Eliminado archivo local: {file}")
                        except OSError as del_err:
                            logger.warning(f"  ⚠️ No se pudo eliminar localmente {file}: {del_err}")

                except (BotoCoreError, ClientError) as e:
                    logger.error(f"  ❌ Error subiendo {file.name} a s3://{BUCKET}/{s3_key}: {e}", exc_info=True)
                    errors += 1
                    continue
            else:
                total_uploaded += 1

    summary_msg = f"Completado: {total_uploaded} archivos procesados, {total_deleted} eliminados localmente, {errors} errores."
    logger.info(summary_msg)

    # Subir el log a S3 para auditoría y monitoreo remoto
    if not dry_run and LOG_FILE.exists():
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        log_s3_key = f"{PREFIX}logs/s3_upload_{timestamp}.log"
        try:
            s3.upload_file(str(LOG_FILE), BUCKET, log_s3_key)
            logger.info(f"📋 Log de subida sincronizado a s3://{BUCKET}/{log_s3_key}")
        except Exception as log_err:
            logger.warning(f"No se pudo sincronizar el log a S3: {log_err}")

    if errors > 0:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Sube CSVs al bucket S3 y elimina locales")
    parser.add_argument("--dry-run", action="store_true", help="Muestra qué subiría sin realizar acciones")
    parser.add_argument("--keep-local", action="store_true", help="Conserva los archivos CSV locales tras la subida")
    args = parser.parse_args()

    upload(args.dry_run, args.keep_local)


if __name__ == "__main__":
    main()
