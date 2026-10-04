#!/usr/bin/env python3
"""
download_faostat.py — Descarga datos de producción agrícola de FAOSTAT para Bolivia y Colombia.

Usa el endpoint de descarga bulk CSV de FAOSTAT (más estable que la API REST).
URL: https://bulks-faostat.fao.org/production/Production_Crops_Livestock_E_Americas.zip

Cultivos/productos: Soybeans, Cattle, Sugar cane, Oil palm fruit
Elementos: Production quantity (t), Area harvested (ha)

Salida: data/external/faostat/faostat_production.csv
"""

import csv
import io
import zipfile
import requests
from pathlib import Path

OUT_DIR = Path(__file__).parent.parent / "data" / "external" / "faostat"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Bulk CSV de Américas — incluye BOL y COL
BULK_URL = "https://bulks-faostat.fao.org/production/Production_Crops_Livestock_E_Americas.zip"

COUNTRY_NAMES = {"Bolivia (Plurinational State of)", "Colombia"}
COUNTRY_ISO   = {"Bolivia (Plurinational State of)": "BOL", "Colombia": "COL"}

ITEMS = {"Soybeans", "Cattle", "Sugar cane", "Oil palm fruit"}
ELEMENTS = {"Production", "Area harvested"}
ELEMENT_COL = {"Production": "production_tonnes", "Area harvested": "area_harvested_ha"}

START_YEAR = 2015
END_YEAR   = 2024


def main():
    out_path = OUT_DIR / "faostat_production.csv"
    fieldnames = ["country_code", "country_name", "year",
                  "item_name", "element", "value", "unit"]

    print(f"  Descargando bulk FAOSTAT Américas...", end=" ", flush=True)
    r = requests.get(BULK_URL, timeout=120)
    r.raise_for_status()
    print(f"{len(r.content)/1e6:.1f} MB")

    written = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            # El archivo principal dentro del zip
            csv_name = next(n for n in z.namelist() if n.endswith(".csv") and "Flag" not in n)
            print(f"  Procesando {csv_name}...")

            with z.open(csv_name) as raw:
                reader = csv.DictReader(io.TextIOWrapper(raw, encoding="latin-1"))
                for row in reader:
                    country = row.get("Area", "")
                    item    = row.get("Item", "")
                    element = row.get("Element", "")

                    if country not in COUNTRY_NAMES:
                        continue
                    if item not in ITEMS:
                        continue
                    if element not in ELEMENTS:
                        continue

                    for year in range(START_YEAR, END_YEAR + 1):
                        val_key = f"Y{year}"
                        val = row.get(val_key, "").strip()
                        if not val:
                            continue
                        try:
                            val_float = float(val.replace(",", ""))
                        except ValueError:
                            continue

                        writer.writerow({
                            "country_code": COUNTRY_ISO[country],
                            "country_name": country,
                            "year":         year,
                            "item_name":    item,
                            "element":      ELEMENT_COL[element],
                            "value":        val_float,
                            "unit":         row.get("Unit", ""),
                        })
                        written += 1

    print(f"  {written:,} filas escritas")
    print(f"\n✅ Guardado en {out_path}")


if __name__ == "__main__":
    main()
