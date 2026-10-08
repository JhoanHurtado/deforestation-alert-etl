#!/usr/bin/env python3
"""
download_worldbank.py — Descarga indicadores del World Bank para Bolivia y Colombia.

Indicadores descargados:
  NY.GDP.MKTP.CD  — PIB (USD corrientes)
  SP.RUR.TOTL     — Población rural
  AG.LND.AGRI.ZS  — Tierra agrícola (% del territorio)
  TX.VAL.AGRI.ZS.UN — Exportaciones agrícolas (% del total)

Salida: data/external/worldbank/worldbank_indicators.csv
"""

import csv
import requests
from pathlib import Path

OUT_DIR = Path(__file__).parent.parent / "data" / "external" / "worldbank"
OUT_DIR.mkdir(parents=True, exist_ok=True)

COUNTRIES  = ["BOL", "COL"]
INDICATORS = {
    "NY.GDP.MKTP.CD":    "gdp_usd",
    "SP.RUR.TOTL":       "rural_population",
    "AG.LND.AGRI.ZS":   "agricultural_land_pct",
    "TX.VAL.AGRI.ZS.UN": "agri_exports_pct",
}
START_YEAR = 2015
END_YEAR   = 2024
BASE_URL   = "https://api.worldbank.org/v2"


def fetch_indicator(country: str, indicator: str) -> list[dict]:
    rows, page = [], 1
    while True:
        r = requests.get(
            f"{BASE_URL}/country/{country}/indicator/{indicator}",
            params={"format": "json", "per_page": 100, "page": page,
                    "date": f"{START_YEAR}:{END_YEAR}"},
            timeout=30,
        )
        r.raise_for_status()
        meta, data = r.json()
        if not data:
            break
        rows.extend(data)
        if page >= meta["pages"]:
            break
        page += 1
    return rows


def main():
    out_path = OUT_DIR / "worldbank_indicators.csv"
    fieldnames = ["country_code", "country_name", "year", "indicator_code",
                  "indicator_name", "value"]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for country in COUNTRIES:
            for indicator_code, col_name in INDICATORS.items():
                print(f"  {country} / {indicator_code}...", end=" ", flush=True)
                try:
                    rows = fetch_indicator(country, indicator_code)
                    written = 0
                    for row in rows:
                        if row.get("value") is None:
                            continue
                        writer.writerow({
                            "country_code":   country,
                            "country_name":   row["country"]["value"],
                            "year":           int(row["date"]),
                            "indicator_code": indicator_code,
                            "indicator_name": col_name,
                            "value":          row["value"],
                        })
                        written += 1
                    print(f"{written} filas")
                except Exception as e:
                    print(f"ERROR: {e}")

    print(f"\n✅ Guardado en {out_path}")


if __name__ == "__main__":
    main()
