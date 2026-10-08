#!/usr/bin/env python3
"""
sync_alerts.py — Sincronización incremental de alertas GFW

Modos de uso:
  python sync_alerts.py                        # descarga datos faltantes hasta hoy
  python sync_alerts.py --from 2026-01-01      # fuerza inicio desde esa fecha
  python sync_alerts.py --from 2026-01-01 --to 2026-06-30  # rango fijo
  python sync_alerts.py --daily                # solo descarga el día de ayer (cron)
  python sync_alerts.py --country BOL          # solo un país

Cron diario (ejecutar a las 06:00):
  0 6 * * * /ruta/al/venv/bin/python /ruta/al/proyecto/sync_alerts.py --daily >> /ruta/logs/sync.log 2>&1
"""

import os
import sys
import csv
import json
import time
import argparse
import requests
from pathlib import Path
from datetime import date, timedelta

# ── Configuración ─────────────────────────────────────────────────────────────

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

API_KEY  = os.getenv("GFW_API_KEY")
BASE_URL = "https://data-api.globalforestwatch.org"
DATASET  = "gfw_integrated_alerts"
VERSION  = "latest"
DATA_DIR = Path(__file__).parent.parent / "data" / "csv"
LOG_DIR  = Path(__file__).parent.parent / "data" / "logs"

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {"x-api-key": API_KEY}

FIELDS = (
    "latitude, longitude, "
    "gfw_integrated_alerts__date, "
    "gfw_integrated_alerts__confidence, "
    "gfw_integrated_alerts__intensity, "
    "umd_tree_cover_density_2000__percent, "
    "is__umd_regional_primary_forest_2001, "
    "wdpa_protected_areas__iucn_cat, "
    "esa_land_cover_2015__class, "
    "wri_google_tree_cover_loss_drivers__category, "
    "gadm_administrative_boundaries__adm1, "
    "is__umd_soy_planted_area_buffered_10km"
)

COUNTRIES = {
    "BOL": {
        "name": "Bolivia",
        "high_density_months": {7, 8, 9, 10, 11},
        "high_density_window": 3,
        "normal_window": 7,
    },
    "COL": {
        "name": "Colombia",
        "high_density_months": {1, 2, 7, 8},
        "high_density_window": 3,
        "normal_window": 7,
    },
}

LIMIT_PER_WINDOW = 2000
MAX_RETRIES      = 3

# ── Helpers ───────────────────────────────────────────────────────────────────

def log(msg):
    ts = date.today().isoformat()
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_DIR / "sync.log", "a") as f:
        f.write(line + "\n")


def check_api_key():
    if not API_KEY:
        log("❌ GFW_API_KEY no encontrada. Revisa tu archivo .env")
        sys.exit(1)


def last_date_in_csv(csv_path: Path) -> date | None:
    """Lee la última fecha registrada en el CSV sin cargarlo completo en memoria."""
    if not csv_path.exists():
        return None
    last = None
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            val = row.get("gfw_integrated_alerts__date", "")
            if val:
                try:
                    d = date.fromisoformat(val[:10])
                    if last is None or d > last:
                        last = d
                except ValueError:
                    pass
    return last


def csv_path_for(country_code: str, start: str, end: str) -> Path:
    return DATA_DIR / f"{country_code.lower()}_alerts_{start}_{end}.csv"


def find_existing_csv(country_code: str) -> Path | None:
    """Busca cualquier CSV existente para el país."""
    pattern = f"{country_code.lower()}_alerts_*.csv"
    files = sorted(DATA_DIR.glob(pattern))
    return files[-1] if files else None


# ── Descarga ──────────────────────────────────────────────────────────────────

def fetch_window(start: str, end: str, country_code: str, retries=MAX_RETRIES) -> list | None:
    sql = (
        f"SELECT {FIELDS} FROM data "
        f"WHERE gfw_integrated_alerts__date >= '{start}' "
        f"AND gfw_integrated_alerts__date <= '{end}' "
        f"LIMIT {LIMIT_PER_WINDOW}"
    )
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(
                f"{BASE_URL}/dataset/{DATASET}/{VERSION}/download_by_aoi/json",
                params={"sql": sql, "aoi[type]": "admin", "aoi[country]": country_code},
                headers=HEADERS,
                timeout=130,
            )
            if r.status_code == 200:
                data = r.json()
                return data if isinstance(data, list) else data.get("data", [])
            if r.status_code == 504 and attempt < retries:
                log(f"   ⏳ 504 — reintento {attempt}/{retries}...")
                time.sleep(5 * attempt)
            else:
                log(f"   ❌ HTTP {r.status_code} en {start}→{end} (intento {attempt})")
                return None
        except requests.exceptions.Timeout:
            log(f"   ⏳ Timeout — reintento {attempt}/{retries}...")
            time.sleep(5 * attempt)
    return None


def windows(start: str, end: str, cfg: dict):
    """Genera ventanas adaptativas según el mes."""
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    while s <= e:
        w_size = cfg["high_density_window"] if s.month in cfg["high_density_months"] else cfg["normal_window"]
        w_end  = min(s + timedelta(days=w_size - 1), e)
        yield s.isoformat(), w_end.isoformat()
        s = w_end + timedelta(days=1)


def split(ws: str, we: str) -> list[tuple]:
    s, e = date.fromisoformat(ws), date.fromisoformat(we)
    if (e - s).days < 1:
        return []
    mid = s + timedelta(days=(e - s).days // 2)
    return [(ws, mid.isoformat()), ((mid + timedelta(days=1)).isoformat(), we)]


# ── Sincronización por país ───────────────────────────────────────────────────

def sync_country(country_code: str, from_date: str, to_date: str) -> dict:
    """
    Descarga alertas de `from_date` a `to_date` para el país dado.
    Añade filas al CSV existente (o crea uno nuevo con el rango completo).
    Retorna un resumen {rows_added, failed_windows}.
    """
    cfg      = COUNTRIES[country_code]
    existing = find_existing_csv(country_code)

    # Determinar archivo de salida: reutilizar el existente o crear uno nuevo
    if existing:
        out_path = existing
        append   = True
    else:
        out_path = csv_path_for(country_code, from_date, to_date)
        append   = False

    failed_path = DATA_DIR / f"failed_windows_{country_code}.json"
    win_list    = list(windows(from_date, to_date, cfg))

    log(f"{'='*55}")
    log(f"🌎 {cfg['name']} ({country_code})")
    log(f"📅 {from_date} → {to_date}  ({len(win_list)} ventanas)")
    log(f"📄 Archivo: {out_path.name}  ({'append' if append else 'nuevo'})")

    rows_added   = 0
    still_failed = {}

    mode = "a" if append else "w"
    with open(out_path, mode, newline="", encoding="utf-8") as f:
        writer = None

        def write_rows(rows):
            nonlocal writer, rows_added
            if writer is None:
                writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                if not append:
                    writer.writeheader()
            writer.writerows(rows)
            rows_added += len(rows)

        def process(ws, we, label):
            days = (date.fromisoformat(we) - date.fromisoformat(ws)).days + 1
            print(f"  {label} {ws}→{we} ({days}d)... ", end="", flush=True)
            rows = fetch_window(ws, we, country_code)
            if rows is None:
                halves = split(ws, we)
                if not halves:
                    print("❌ omitida")
                    still_failed[ws] = we
                    return
                print(f"⚠️  dividiendo")
                time.sleep(3)
                for h0, h1 in halves:
                    process(h0, h1, f"  ↳{label}")
                return
            if not rows:
                print("sin datos")
                return
            write_rows(rows)
            print(f"{len(rows)} filas")

        for i, (ws, we) in enumerate(win_list, 1):
            process(ws, we, f"[{i:03d}/{len(win_list)}]")

    # Guardar ventanas fallidas
    existing_failed = {}
    if failed_path.exists():
        with open(failed_path) as fp:
            existing_failed = json.load(fp)
    existing_failed.update(still_failed)
    with open(failed_path, "w") as fp:
        json.dump(existing_failed, fp, indent=2)

    log(f"✅ {cfg['name']}: +{rows_added:,} filas  |  fallidas: {len(still_failed)}")
    return {"rows_added": rows_added, "failed": len(still_failed)}


# ── Lógica principal ──────────────────────────────────────────────────────────

def resolve_range(country_code: str, args) -> tuple[str, str] | None:
    """
    Determina (from_date, to_date) según los argumentos y el estado actual del CSV.
    Retorna None si no hay nada que descargar.
    """
    today     = date.today()
    yesterday = today - timedelta(days=1)

    if args.daily:
        return yesterday.isoformat(), yesterday.isoformat()

    if args.to:
        to_date = date.fromisoformat(args.to)
    else:
        to_date = yesterday  # nunca descargar "hoy" (datos incompletos)

    if args.from_date:
        from_date = date.fromisoformat(args.from_date)
    else:
        # Detectar automáticamente la última fecha en el CSV
        existing = find_existing_csv(country_code)
        last     = last_date_in_csv(existing) if existing else None
        if last:
            from_date = last + timedelta(days=1)
            log(f"   {country_code}: última fecha en CSV = {last}  →  descargando desde {from_date}")
        else:
            log(f"   {country_code}: no hay CSV previo localmente. Intentando descargar base desde el proxy S3...")
            try:
                import subprocess
                fetch_script = Path(__file__).parent / "fetch_from_proxy.py"
                if fetch_script.exists():
                    subprocess.run([sys.executable, str(fetch_script), "--only", "gfw"], check=True)
                    existing = find_existing_csv(country_code)
                    last = last_date_in_csv(existing) if existing else None
                    if last:
                        from_date = last + timedelta(days=1)
                        log(f"   {country_code}: CSV base obtenido desde proxy (última fecha: {last})")
            except Exception as e:
                log(f"   ⚠️ Error descargando base desde proxy: {e}")
            if not last:
                log(f"   {country_code}: no hay CSV previo — se omite descarga histórica pesada la primera vez (usar fetch_from_proxy.py o --from YYYY-MM-DD)")
                return None

    if from_date > to_date:
        log(f"   {country_code}: ya está al día ({from_date} > {to_date}), nada que descargar")
        return None

    return from_date.isoformat(), to_date.isoformat()


def main():
    parser = argparse.ArgumentParser(
        description="Sincronización incremental de alertas GFW",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--from",    dest="from_date", metavar="YYYY-MM-DD",
                        help="Fecha de inicio (sobreescribe detección automática)")
    parser.add_argument("--to",      dest="to",        metavar="YYYY-MM-DD",
                        help="Fecha de fin (default: ayer)")
    parser.add_argument("--daily",   action="store_true",
                        help="Descarga solo el día de ayer (modo cron)")
    parser.add_argument("--country", metavar="CODE",
                        help="Solo un país (BOL o COL)")
    args = parser.parse_args()

    check_api_key()

    targets = [args.country.upper()] if args.country else list(COUNTRIES.keys())
    invalid = [c for c in targets if c not in COUNTRIES]
    if invalid:
        log(f"❌ País(es) no reconocido(s): {invalid}. Opciones: {list(COUNTRIES.keys())}")
        sys.exit(1)

    log(f"🚀 sync_alerts.py  |  países: {targets}  |  daily={args.daily}")

    total_added = 0
    for code in targets:
        rng = resolve_range(code, args)
        if rng is None:
            continue
        result = sync_country(code, rng[0], rng[1])
        total_added += result["rows_added"]

    log(f"{'='*55}")
    log(f"🏁 Total filas añadidas: {total_added:,}")


if __name__ == "__main__":
    main()
