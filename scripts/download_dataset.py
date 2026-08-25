"""
Extracción de alertas de deforestación desde Global Forest Watch API.
Dataset: gfw_integrated_alerts (nivel de evento, 12 columnas)
Países: Bolivia (BOL), Colombia (COL)
Cada país se guarda en su propio CSV.
Requiere: GFW_API_KEY en archivo .env
"""
import os
import json
import csv
import time
import requests
from pathlib import Path
from datetime import date, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

API_KEY  = os.getenv("GFW_API_KEY")
BASE_URL = "https://data-api.globalforestwatch.org"
DATASET  = "gfw_integrated_alerts"
VERSION  = "latest"
DATA_DIR = Path("data/csv/")
DATA_DIR.mkdir(parents=True, exist_ok=True)

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

LIMIT_PER_WINDOW = 2000
MAX_RETRIES      = 3
MIN_WINDOW_DAYS  = 1  # tamaño mínimo antes de abandonar una ventana

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

START_DATE = "2022-01-01"
END_DATE   = "2026-07-31"
# ─────────────────────────────────────────────────────────────────────────────


def check_api_key():
    if not API_KEY:
        print("❌ GFW_API_KEY no encontrada. Revisa tu archivo .env")
        exit(1)
    requests.get(f"{BASE_URL}/ping").raise_for_status()
    print("✅ API disponible.")


def fetch_json(start_date, end_date, country_code, limit, retries=MAX_RETRIES):
    """Descarga hasta `limit` filas en JSON para un país. Reintenta en 504."""
    sql = (
        f"SELECT {FIELDS} FROM data "
        f"WHERE gfw_integrated_alerts__date >= '{start_date}' "
        f"AND gfw_integrated_alerts__date <= '{end_date}' "
        f"LIMIT {limit}"
    )
    for attempt in range(1, retries + 1):
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
            print(f"   ⏳ 504 — reintento {attempt}/{retries - 1}...")
            time.sleep(5)
        else:
            print(f"   ❌ {start_date}→{end_date}: error {r.status_code} (intento {attempt})")
            return None
    return None


def week_windows(start_date, end_date, high_density_months, high_density_window, normal_window):
    """Genera ventanas adaptativas según el mes."""
    s = date.fromisoformat(start_date)
    e = date.fromisoformat(end_date)
    while s <= e:
        window = high_density_window if s.month in high_density_months else normal_window
        w = min(s + timedelta(days=window - 1), e)
        yield s.isoformat(), w.isoformat()
        s = w + timedelta(days=1)


def split_window(ws, we):
    """Divide una ventana en dos mitades. Retorna [] si ya es de 1 día."""
    s = date.fromisoformat(ws)
    e = date.fromisoformat(we)
    delta = (e - s).days
    if delta < 1:
        return []
    mid = s + timedelta(days=delta // 2)
    return [(ws, mid.isoformat()), ((mid + timedelta(days=1)).isoformat(), we)]


def preview_alerts(country_code="BOL", start_date="2024-06-01", end_date="2024-06-07", limit=100):
    """Descarga hasta limit filas, muestra estructura y guarda data/preview_{country}.json."""
    print(f"   Ventana: {start_date} → {end_date}, LIMIT {limit}")
    rows = fetch_json(start_date, end_date, country_code, limit)
    if not rows:
        print("   ⚠️ No se devolvieron filas.")
        return []

    print(f"📋 Preview ({country_code}): {len(rows)} filas devueltas")
    print("   Columnas:", list(rows[0].keys()))
    print(f"   Total columnas: {len(rows[0].keys())}")
    print("   Ejemplo de fila:", rows[0])

    preview_path = DATA_DIR / f"preview_{country_code}.json"
    with open(preview_path, "w") as f:
        json.dump(rows, f, indent=2)
    print(f"   💾 Preview guardado en {preview_path}")
    return rows


def retry_failed_windows(country_code, start_date=START_DATE, end_date=END_DATE):
    """
    Lee failed_windows_{country}.json y reintenta cada ventana fallida,
    añadiendo filas al CSV existente. Actualiza el archivo de fallidas.
    """
    failed_path = DATA_DIR / f"failed_windows_{country_code}.json"
    out_path    = DATA_DIR / f"{country_code.lower()}_alerts_{start_date}_{end_date}.csv"

    if not failed_path.exists():
        print(f"   ℹ️  No hay archivo de ventanas fallidas para {country_code}.")
        return
    with open(failed_path) as f:
        failed = json.load(f)
    if not failed:
        print(f"   ✅ No hay ventanas fallidas pendientes para {country_code}.")
        return

    print(f"\n🔄 Reintentando {len(failed)} ventanas fallidas para {COUNTRIES[country_code]['name']}...")
    still_failed = {}
    recovered    = 0

    with open(out_path, "a", newline="", encoding="utf-8") as out_f:
        writer = None

        def write_rows(rows):
            nonlocal writer, recovered
            if writer is None:
                writer = csv.DictWriter(out_f, fieldnames=list(rows[0].keys()))
            writer.writerows(rows)
            recovered += len(rows)

        def process_window(ws, we, label):
            days = (date.fromisoformat(we) - date.fromisoformat(ws)).days + 1
            print(f"📡 {label} {ws} → {we} ({days}d)...", end=" ", flush=True)
            rows = fetch_json(ws, we, country_code, LIMIT_PER_WINDOW)
            if rows is None:
                halves = split_window(ws, we)
                if not halves:
                    print("❌ ventana mínima — omitida")
                    still_failed[ws] = {"end": we}
                    return
                half_days = days // 2
                print(f"⚠️  fallo — dividiendo ({days}d → {half_days}d)")
                time.sleep(5)
                for h_start, h_end in halves:
                    process_window(h_start, h_end, f"  ↳ {label}")
                return
            if not rows:
                print("sin datos")
                return
            write_rows(rows)
            print(f"{len(rows)} filas")

        for i, (ws, v) in enumerate(failed.items(), 1):
            process_window(ws, v["end"], f"[retry {i:02d}/{len(failed)}]")

    with open(failed_path, "w") as f:
        json.dump(still_failed, f, indent=2)

    print(f"   Filas recuperadas : {recovered:,}")
    print(f"   Siguen fallidas   : {len(still_failed)}")


def download_country_alerts(country_code, start_date=START_DATE, end_date=END_DATE):
    """
    Descarga completa para un país, ventana a ventana.
    - Si una ventana falla, la divide en mitades y reintenta recursivamente
      hasta llegar a MIN_WINDOW_DAYS antes de abandonarla.
    - Guarda ventanas irrecuperables en failed_windows_{country}.json.
    - Retorna (out_path, total_rows)
    """
    cfg     = COUNTRIES[country_code]
    windows = list(week_windows(
        start_date, end_date,
        cfg["high_density_months"],
        cfg["high_density_window"],
        cfg["normal_window"],
    ))

    out_path     = DATA_DIR / f"{country_code.lower()}_alerts_{start_date}_{end_date}.csv"
    failed_path  = DATA_DIR / f"failed_windows_{country_code}.json"
    writer       = None
    total_rows   = 0
    still_failed = {}

    print(f"\n{'='*60}")
    print(f"🌎 {cfg['name']} ({country_code})")
    print(f"📅 {len(windows)} ventanas adaptativas ({start_date} → {end_date})")
    print(f"   Normal: {cfg['normal_window']}d | Alta densidad: {cfg['high_density_window']}d en meses {sorted(cfg['high_density_months'])}")
    print(f"   LIMIT {LIMIT_PER_WINDOW} filas/ventana → hasta {len(windows) * LIMIT_PER_WINDOW:,} filas totales\n")

    with open(out_path, "w", newline="", encoding="utf-8") as out_f:

        def write_rows(rows):
            nonlocal writer, total_rows
            if writer is None:
                writer = csv.DictWriter(out_f, fieldnames=list(rows[0].keys()))
                writer.writeheader()
            writer.writerows(rows)
            total_rows += len(rows)

        def process_window(ws, we, label):
            """Intenta descargar la ventana; si falla, la divide recursivamente."""
            days = (date.fromisoformat(we) - date.fromisoformat(ws)).days + 1
            print(f"📡 {label} {ws} → {we} ({days}d)...", end=" ", flush=True)
            rows = fetch_json(ws, we, country_code, LIMIT_PER_WINDOW)

            if rows is None:
                halves = split_window(ws, we)
                if not halves:
                    print(f"❌ ventana mínima — omitida")
                    still_failed[ws] = {"end": we}
                    return
                half_days = days // 2
                print(f"⚠️  fallo — dividiendo ({days}d → {half_days}d)")
                time.sleep(5)
                for h_start, h_end in halves:
                    process_window(h_start, h_end, f"  ↳ {label}")
                return

            if not rows:
                print("sin datos")
                return
            write_rows(rows)
            print(f"{len(rows)} filas")

        for i, (ws, we) in enumerate(windows, 1):
            process_window(ws, we, f"[{i:03d}/{len(windows)}]")

    with open(failed_path, "w") as f:
        json.dump(still_failed, f, indent=2)

    print(f"\n{'='*60}")
    print(f"✅ {cfg['name']} guardado en {out_path}")
    print(f"   Registros totales : {total_rows:,}")
    print(f"   Ventanas base     : {len(windows)}")
    print(f"   Ventanas fallidas : {len(still_failed)}")
    if still_failed:
        print(f"   ⚠️  Detalle en     : {failed_path}")
        for ws, v in still_failed.items():
            print(f"      {ws} → {v['end']}")
    print(f"{'='*60}")

    return out_path, total_rows


def main_menu():
    print("\n" + "="*60)
    print("  🌿 GFW Deforestation Alerts Downloader")
    print("="*60)
    print("  1. Preview (estructura de datos, 1 semana)")
    print("  2. Descarga completa")
    print("  3. Reintentar ventanas fallidas")
    print("  0. Salir")
    return input("\nOpción: ").strip()


if __name__ == "__main__":
    check_api_key()

    while True:
        opcion = main_menu()

        if opcion == "0":
            print("Saliendo.")
            break

        elif opcion == "1":
            print("\n🔍 Preview de cada país (1 semana, LIMIT 100)...")
            for code in COUNTRIES:
                preview_alerts(country_code=code, start_date="2024-06-01", end_date="2024-06-07", limit=100)

        elif opcion == "2":
            confirm = input(f"\nDescargar {', '.join(COUNTRIES.keys())} ({START_DATE} → {END_DATE})? (s/n): ")
            if confirm.lower() != "s":
                print("Cancelado.")
                continue
            summary = {}
            for code in COUNTRIES:
                out_path, row_count = download_country_alerts(code, START_DATE, END_DATE)
                summary[code] = {"path": str(out_path), "rows": row_count}
            print("\n" + "="*60)
            print("📊 RESUMEN GLOBAL")
            print("="*60)
            total = 0
            for code, info in summary.items():
                name = COUNTRIES[code]["name"]
                print(f"   {name:12s} ({code}): {info['rows']:>10,} filas → {info['path']}")
                total += info["rows"]
            print(f"   {'TOTAL':17s}: {total:>10,} filas")
            print("="*60)
            if total < 10000:
                print(f"\n⚠️ Solo {total:,} filas. Algunas ventanas fallaron — usa opción 3.")
            else:
                print(f"\n🎉 {total:,} filas totales — supera el mínimo de 10,000 requerido.")

        elif opcion == "3":
            # Mostrar qué países tienen ventanas fallidas
            pendientes = []
            for code in COUNTRIES:
                fp = DATA_DIR / f"failed_windows_{code}.json"
                if fp.exists():
                    with open(fp) as f:
                        data = json.load(f)
                    if data:
                        pendientes.append((code, len(data)))
            if not pendientes:
                print("\n✅ No hay ventanas fallidas pendientes en ningún país.")
                continue
            print("\nPaíses con ventanas fallidas:")
            for code, n in pendientes:
                print(f"   {code} — {COUNTRIES[code]['name']}: {n} ventanas")
            print("   all — Reintentar todos")
            sel = input("\n¿Cuál reintentar? (código o 'all'): ").strip().upper()
            targets = [c for c, _ in pendientes] if sel == "ALL" else ([sel] if sel in COUNTRIES else [])
            if not targets:
                print("Opción no válida.")
                continue
            for code in targets:
                retry_failed_windows(code, START_DATE, END_DATE)

        else:
            print("Opción no válida.")
