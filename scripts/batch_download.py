#!/usr/bin/env python3
"""
batch_download.py — Orquesta la descarga de todos los datasets y sube a S3.

Ejecuta en orden:
  1. sync_alerts.py --daily          (GFW — alertas del día anterior)
  2. download_worldbank.py           (World Bank — indicadores económicos)
  3. download_faostat.py             (FAO — producción agrícola)
  4. download_geonames.py            (GeoNames — localidades pobladas)
  5. upload_to_s3.py                 (sube todo al bucket S3)

Cron diario en Lightsail (06:00 UTC):
  0 6 * * * /home/ubuntu/venv/bin/python /home/ubuntu/deforestation-alert-etl/scripts/batch_download.py >> /home/ubuntu/logs/batch.log 2>&1

Uso manual:
  python scripts/batch_download.py
  python scripts/batch_download.py --skip-gfw        # omite descarga GFW
  python scripts/batch_download.py --skip-upload     # no sube a S3
"""

import sys
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

SCRIPTS_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPTS_DIR.parent

# Buscar el Python del venv del proyecto, sin importar cómo se invocó el script
_venv_python = PROJECT_DIR / 'venv' / 'bin' / 'python'
PYTHON = _venv_python if _venv_python.exists() else Path(sys.executable).resolve()


def run(script: str, extra_args: list[str] = []) -> bool:
    path = SCRIPTS_DIR / script
    cmd  = [str(PYTHON), str(path)] + extra_args
    print(f"\n{'='*55}")
    print(f"▶  {script}  {' '.join(extra_args)}")
    print(f"{'='*55}")
    result = subprocess.run(cmd, cwd=SCRIPTS_DIR.parent)
    if result.returncode != 0:
        print(f"⚠️  {script} terminó con código {result.returncode}")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description="Batch download de todos los datasets")
    parser.add_argument("--skip-gfw",    action="store_true", help="Omite sync_alerts.py")
    parser.add_argument("--skip-upload", action="store_true", help="Omite upload_to_s3.py")
    parser.add_argument("--full-gfw",    action="store_true", help="Descarga GFW completo (no solo --daily)")
    args = parser.parse_args()

    start = datetime.now()
    print(f"🚀 batch_download.py — {start.strftime('%Y-%m-%d %H:%M:%S')}")

    results = {}

    if not args.skip_gfw:
        gfw_args = [] if args.full_gfw else ["--daily"]
        results["gfw"]        = run("sync_alerts.py", gfw_args)

    results["worldbank"]  = run("download_worldbank.py")
    results["faostat"]    = run("download_faostat.py")
    results["geonames"]   = run("download_geonames.py")

    if not args.skip_upload:
        results["s3_upload"]  = run("upload_to_s3.py")

    elapsed = (datetime.now() - start).seconds
    print(f"\n{'='*55}")
    print(f"🏁 Completado en {elapsed}s")
    for name, ok in results.items():
        status = "✅" if ok else "❌"
        print(f"   {status}  {name}")
    print(f"{'='*55}")

    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
