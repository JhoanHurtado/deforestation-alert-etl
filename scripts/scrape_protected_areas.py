#!/usr/bin/env python3
"""
scrape_protected_areas.py — Scraping de áreas protegidas de Bolivia y Colombia.

Fuente: Wikipedia
  - https://en.wikipedia.org/wiki/List_of_protected_areas_of_Bolivia
  - https://en.wikipedia.org/wiki/List_of_national_parks_of_Colombia

Extrae: nombre, tipo, área (km²), año de establecimiento, departamento/región.
Salida: data/external/protected_areas/protected_areas.csv

Uso:
  python scripts/scrape_protected_areas.py
  python scripts/scrape_protected_areas.py --dry-run
"""

import csv
import re
import time
import argparse
from pathlib import Path

import requests
from bs4 import BeautifulSoup

OUT_DIR = Path(__file__).parent.parent / "data" / "external" / "protected_areas"
OUT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
PAUSE_SEC = 1.5

TARGETS = [
    {
        "country_code": "BOL",
        "country_name": "Bolivia",
        "url": "https://en.wikipedia.org/wiki/National_parks_(Bolivia)",
    },
    {
        "country_code": "COL",
        "country_name": "Colombia",
        "url": "https://en.wikipedia.org/wiki/List_of_national_parks_of_Colombia",
    },
]

FIELDNAMES = ["country_code", "country_name", "name", "type",
              "area_km2", "established_year", "region", "source_url"]


def fetch_soup(url: str) -> BeautifulSoup | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return BeautifulSoup(r.text, "lxml")
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return None


def clean(s: str) -> str:
    s = re.sub(r"\[.*?\]", "", s)          # quitar referencias [1]
    s = s.replace("\xa0", " ").strip()
    return " ".join(s.split())             # colapsar espacios


def parse_number(s: str) -> str:
    s = re.sub(r"\[.*?\]", "", s)
    m = re.search(r"[\d,]+\.?\d*", s.replace(",", ""))
    return m.group(0) if m else ""


def parse_table(table, country_code: str, country_name: str, url: str) -> list[dict]:
    rows = []

    # Extraer headers de la primera fila con <th>
    header_row = table.find("tr")
    if not header_row:
        return []
    headers = [clean(th.get_text()) for th in header_row.find_all(["th", "td"])]

    # Necesitamos al menos una columna de nombre
    name_keys   = ["Name", "Protected area", "Park", "Area name", "Reserve"]
    area_keys   = ["Area", "Area (km²)", "Area(km²)", "Size (km²)", "Surface"]
    year_keys   = ["Established", "Year", "Created", "Founded"]
    region_keys = ["Department", "Region", "Location", "Province", "State"]
    type_keys   = ["Type", "Category", "IUCN", "Classification"]

    def find_col(keys):
        for k in keys:
            for h in headers:
                if k.lower() in h.lower():
                    return h
        return None

    col_name   = find_col(name_keys)
    col_area   = find_col(area_keys)
    col_year   = find_col(year_keys)
    col_region = find_col(region_keys)
    col_type   = find_col(type_keys)

    if not col_name:
        return []

    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue
        row = {}
        for i, cell in enumerate(cells):
            if i < len(headers):
                row[headers[i]] = clean(cell.get_text())

        name = row.get(col_name, "").strip()
        if not name or name.lower() in ("name", "protected area", "park", ""):
            continue

        rows.append({
            "country_code":     country_code,
            "country_name":     country_name,
            "name":             name,
            "type":             clean(row.get(col_type, "National Park")) if col_type else "National Park",
            "area_km2":         parse_number(row.get(col_area, "")) if col_area else "",
            "established_year": parse_number(row.get(col_year, "")) if col_year else "",
            "region":           clean(row.get(col_region, "")) if col_region else "",
            "source_url":       url,
        })

    return rows


def scrape(target: dict) -> list[dict]:
    url          = target["url"]
    country_code = target["country_code"]
    country_name = target["country_name"]

    soup = fetch_soup(url)
    if soup is None:
        return []

    all_rows = []
    tables = soup.find_all("table", class_="wikitable")
    print(f"  Tablas wikitable encontradas: {len(tables)}")

    for i, table in enumerate(tables):
        rows = parse_table(table, country_code, country_name, url)
        if rows:
            print(f"  Tabla {i+1}: {len(rows)} filas extraídas")
            all_rows.extend(rows)

    return all_rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    all_rows = []

    for target in TARGETS:
        print(f"\n{'='*55}")
        print(f"🌎 {target['country_name']} ({target['country_code']})")
        print(f"   {target['url']}")

        rows = scrape(target)
        print(f"  Total extraído: {len(rows)} áreas")
        if rows:
            print(f"  Ejemplo: {rows[0]['name']} | {rows[0]['type']} | {rows[0]['area_km2']} km²")
        all_rows.extend(rows)
        time.sleep(PAUSE_SEC)

    print(f"\n{'='*55}")
    print(f"Total filas: {len(all_rows)}")

    if args.dry_run:
        print("\n[DRY RUN] Primeras 5 filas:")
        for r in all_rows[:5]:
            print(" ", r)
        return

    out_path = OUT_DIR / "protected_areas.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"✅ Guardado en {out_path}")


if __name__ == "__main__":
    main()
