#!/usr/bin/env python3
"""
upload_to_s3.py — Sube los CSVs descargados al bucket S3 configurado.

Sube:
  data/csv/          → s3://<bucket>/<prefix>gfw/
  data/external/     → s3://<bucket>/<prefix>external/

Uso:
  python scripts/upload_to_s3.py
  python scripts/upload_to_s3.py --dry-run   # muestra qué subiría sin subir
"""

import os
import sys
import argparse
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    print("❌ boto3 no instalado. Ejecuta: pip install boto3")
    sys.exit(1)

BUCKET = os.getenv("S3_BUCKET")
PREFIX = os.getenv("S3_PREFIX", "raw/").rstrip("/") + "/"
REGION = os.getenv("AWS_REGION", "us-east-1")

DATA_ROOT = Path(__file__).parent.parent / "data"

# Mapeo local → prefijo S3
UPLOAD_DIRS = {
    DATA_ROOT / "csv":      f"{PREFIX}gfw/",
    DATA_ROOT / "external": f"{PREFIX}external/",
}

EXTENSIONS = {".csv", ".json"}


def upload(dry_run: bool):
    if not BUCKET:
        print("❌ S3_BUCKET no configurado en .env")
        sys.exit(1)

    s3 = boto3.client("s3", region_name=REGION)
    total = 0

    for local_dir, s3_prefix in UPLOAD_DIRS.items():
        if not local_dir.exists():
            print(f"  ⚠️  {local_dir} no existe — omitido")
            continue
        for file in sorted(local_dir.rglob("*")):
            if file.suffix not in EXTENSIONS or not file.is_file():
                continue
            # Mantener estructura relativa dentro del directorio
            relative = file.relative_to(local_dir)
            s3_key   = s3_prefix + str(relative).replace("\\", "/")
            size_mb  = file.stat().st_size / 1_048_576
            print(f"  {'[DRY]' if dry_run else '↑'} {file.name} ({size_mb:.1f} MB) → s3://{BUCKET}/{s3_key}")
            if not dry_run:
                try:
                    s3.upload_file(str(file), BUCKET, s3_key)
                except (BotoCoreError, ClientError) as e:
                    print(f"    ❌ Error: {e}")
                    continue
            total += 1

    print(f"\n{'[DRY RUN] ' if dry_run else ''}✅ {total} archivos {'procesados' if dry_run else 'subidos'}")


def main():
    parser = argparse.ArgumentParser(description="Sube CSVs al bucket S3")
    parser.add_argument("--dry-run", action="store_true", help="Muestra qué subiría sin subir")
    args = parser.parse_args()
    upload(args.dry_run)


if __name__ == "__main__":
    main()
