#!/usr/bin/env python3
"""
download_geonames.py — Descarga localidades pobladas de Bolivia y Colombia desde GeoNames.

Fuente: https://download.geonames.org/export/dump/
Archivos: BO.zip, CO.zip  (~5 MB total)

Columnas relevantes extraídas:
  geonameid, name, latitude, longitude, feature_class, feature_code,
  country_code, population, elevation, admin1_code

Salida: data/external/geonames/geonames_places.csv
"""

import csv
import io
import zipfile
import requests
from pathlib import Path

OUT_DIR = Path(__file__).parent.parent / "data" / "external" / "geonames"
OUT_DIR.mkdir(parents=True, exist_ok=True)

COUNTRIES = {"BOL": "BO", "COL": "CO"}
BASE_URL  = "https://download.geonames.org/export/dump"

# Columnas del formato GeoNames (tab-separated, sin header)
GEONAMES_COLS = [
    "geonameid", "name", "asciiname", "alternatenames",
    "latitude", "longitude", "feature_class", "feature_code",
    "country_code", "cc2", "admin1_code", "admin2_code",
    "admin3_code", "admin4_code", "population", "elevation",
    "dem", "timezone", "modification_date",
]

KEEP_COLS = ["geonameid", "name", "latitude", "longitude",
             "feature_class", "feature_code", "country_code",
             "admin1_code", "population", "elevation"]

# Solo localidades pobladas (P = populated place)
FEATURE_CLASS_FILTER = {"P"}


def download_country(iso2: str, country_iso3: str, writer: csv.DictWriter):
    url = f"{BASE_URL}/{iso2}.zip"
    print(f"  Descargando {url}...", end=" ", flush=True)
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    written = 0
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        with z.open(f"{iso2}.txt") as txt:
            reader = csv.DictReader(
                io.TextIOWrapper(txt, encoding="utf-8"),
                fieldnames=GEONAMES_COLS,
                delimiter="\t",
            )
            for row in reader:
                if row["feature_class"] not in FEATURE_CLASS_FILTER:
                    continue
                out = {col: row[col] for col in KEEP_COLS}
                out["country_iso3"] = country_iso3
                writer.writerow(out)
                written += 1
    print(f"{written:,} localidades")


def main():
    out_path = OUT_DIR / "geonames_places.csv"
    fieldnames = KEEP_COLS + ["country_iso3"]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for iso3, iso2 in COUNTRIES.items():
            try:
                download_country(iso2, iso3, writer)
            except Exception as e:
                print(f"ERROR {iso3}: {e}")

    print(f"\n✅ Guardado en {out_path}")


if __name__ == "__main__":
    main()
