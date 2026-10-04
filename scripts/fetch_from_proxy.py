#!/usr/bin/env python3
"""
fetch_from_proxy.py — Descarga los CSVs desde el proxy inverso (S3) al entorno local.

El notebook llama a este script (o lo importa) antes de leer los CSVs.
La URL base se configura en DATA_PROXY_URL del .env.

Archivos esperados en el proxy:
  <proxy>/gfw/bol_alerts_<start>_<end>.csv
  <proxy>/gfw/col_alerts_<start>_<end>.csv
  <proxy>/external/worldbank/worldbank_indicators.csv
  <proxy>/external/faostat/faostat_production.csv
  <proxy>/external/geonames/geonames_places.csv

Uso desde notebook:
  import subprocess, sys
  subprocess.run([sys.executable, "../scripts/fetch_from_proxy.py"], check=True)

Uso directo:
  python scripts/fetch_from_proxy.py
  python scripts/fetch_from_proxy.py --only gfw          # solo alertas GFW
  python scripts/fetch_from_proxy.py --only external     # solo datasets externos
"""

import os
import sys
import argparse
import requests
from pathlib import Path
from datetime import date

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

PROXY_URL  = os.getenv("DATA_PROXY_URL", "").rstrip("/")
DATA_ROOT  = Path(__file__).parent.parent / "data"
START_DATE = "2022-01-01"
END_DATE   = date.today().isoformat()

# Archivos a descargar: (ruta_s3_relativa, ruta_local)
FILES = {
    "gfw": [
        (f"gfw/bol_alerts_{START_DATE}_{END_DATE}.csv",
         DATA_ROOT / "csv" / f"bol_alerts_{START_DATE}_{END_DATE}.csv"),
        (f"gfw/col_alerts_{START_DATE}_{END_DATE}.csv",
         DATA_ROOT / "csv" / f"col_alerts_{START_DATE}_{END_DATE}.csv"),
    ],
    "external": [
        ("external/worldbank/worldbank_indicators.csv",
         DATA_ROOT / "external" / "worldbank" / "worldbank_indicators.csv"),
        ("external/faostat/faostat_production.csv",
         DATA_ROOT / "external" / "faostat" / "faostat_production.csv"),
        ("external/geonames/geonames_places.csv",
         DATA_ROOT / "external" / "geonames" / "geonames_places.csv"),
    ],
}


def download_file(url: str, dest: Path) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"  ↓ {url.split('/')[-1]}...", end=" ", flush=True)
    try:
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=1_048_576):
                    f.write(chunk)
        size_mb = dest.stat().st_size / 1_048_576
        print(f"{size_mb:.1f} MB ✅")
        return True
    except Exception as e:
        print(f"❌ {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Descarga CSVs desde el proxy S3")
    parser.add_argument("--only", choices=["gfw", "external"],
                        help="Descarga solo un grupo de archivos")
    args = parser.parse_args()

    if not PROXY_URL:
        print("❌ DATA_PROXY_URL no configurado en .env")
        sys.exit(1)

    groups = [args.only] if args.only else list(FILES.keys())
    errors = 0

    for group in groups:
        print(f"\n── {group.upper()} ──")
        for s3_path, local_path in FILES[group]:
            url = f"{PROXY_URL}/{s3_path}"
            if not download_file(url, local_path):
                errors += 1

    print(f"\n{'✅ Todo descargado' if errors == 0 else f'⚠️  {errors} error(es)'}")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
