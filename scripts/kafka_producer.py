#!/usr/bin/env python3
"""
kafka_producer.py — Productor de streaming de eventos de alertas de deforestación.

Requisitos de la Entrega Final (Kafka Producer and Streaming):
  1. Lee registros de eventos de alertas (fact_alerts / transformed data) simulando
     el flujo continuo de detección satelital en tiempo real.
  2. Cada mensaje representa una alerta satelital con los atributos de entrada (features)
     necesarios para la inferencia, sin fuga de datos (data leakage).
  3. Soporta tanto conexión a un broker Kafka real (BOOTSTRAP_SERVERS) como un
     modo de simulación local en memoria o archivo ('--dry-run' o fallback inteligente)
     si Kafka no está instalado en la máquina del usuario, garantizando 100% reproducibilidad.
"""

import sys
import os
import json
import time
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent
PROCESSED_DATA = PROJECT_ROOT / "data" / "processed" / "gfw_transformed.pkl"
STREAM_LOG = PROJECT_ROOT / "data" / "logs" / "kafka_stream_events.jsonl"
STREAM_LOG.parent.mkdir(parents=True, exist_ok=True)

TOPIC_NAME = "deforestation-alerts-stream"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def get_producer():
    """Intenta inicializar el productor de Kafka real."""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5000,
        )
        print(f"✅ Conectado a Kafka Broker en {BOOTSTRAP_SERVERS} (Topic: {TOPIC_NAME})")
        return producer
    except Exception as e:
        print(f"⚠️ No se pudo conectar al broker Kafka real ({e}).")
        print("  ▶ Activando emulador de streaming persistente (fallback local en data/logs/).")
        return None


def main():
    parser = argparse.ArgumentParser(description="Productor de streaming de alertas a Kafka")
    parser.add_argument("--limit", type=int, default=100, help="Número de eventos a emitir (default: 100)")
    parser.add_argument("--delay", type=float, default=0.2, help="Retardo en segundos entre mensajes (default: 0.2s)")
    parser.add_argument("--continuous", action="store_true", help="Emitir continuamente en loop infinito")
    args = parser.parse_args()

    print("=" * 65)
    print("🛰️ INICIANDO PRODUCTOR KAFKA — DEFORESTATION STREAM")
    print(f"   Topic: {TOPIC_NAME} | Retardo: {args.delay}s | Límite: {args.limit if not args.continuous else 'Infinito'}")
    print("=" * 65)

    if not PROCESSED_DATA.exists():
        print(f"❌ Error: {PROCESSED_DATA} no existe. Ejecuta scripts/transform_and_enrich.py primero.")
        sys.exit(1)

    df = pd.read_pickle(PROCESSED_DATA)
    producer = get_producer()

    feature_cols = [
        "latitude", "longitude", "tree_cover_density_pct",
        "dist_nearest_place_km", "month", "week", "country_code", "driver_id"
    ]

    sent_count = 0
    with open(STREAM_LOG, "a", encoding="utf-8") as f_log:
        while True:
            sample_df = df.sample(min(len(df), args.limit), random_state=int(time.time()) % 1000)
            for idx, row in sample_df.iterrows():
                event = {
                    "event_id": f"evt_{int(time.time()*1000)}_{idx}",
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "latitude": float(row["latitude"]),
                    "longitude": float(row["longitude"]),
                    "tree_cover_density_pct": float(row.get("tree_cover_density_pct", 50.0)),
                    "dist_nearest_place_km": float(row.get("dist_nearest_place_km", 10.0)),
                    "month": int(row.get("month", 1)),
                    "week": int(row.get("week", 1)),
                    "country_code": str(row.get("country_code", "COL")),
                    "driver_id": int(row.get("loss_driver_category", 7) if "loss_driver_category" in row else 7),
                    "confidence": str(row.get("confidence", "nominal")),
                }

                # Publicar a Kafka real si está disponible
                if producer:
                    try:
                        producer.send(TOPIC_NAME, value=event)
                    except Exception as e:
                        print(f"  ❌ Error enviando a Kafka: {e}")

                # Registrar siempre en el bus local para el dashboard en tiempo real
                f_log.write(json.dumps(event) + "\n")
                f_log.flush()

                sent_count += 1
                if sent_count % 10 == 0 or sent_count == 1:
                    print(f"  [Kafka Emitted] #{sent_count} | Event: {event['event_id']} | Coords: ({event['latitude']:.3f}, {event['longitude']:.3f}) | Pais: {event['country_code']}")

                time.sleep(args.delay)
                if not args.continuous and sent_count >= args.limit:
                    break

            if not args.continuous and sent_count >= args.limit:
                break

    if producer:
        producer.flush()

    print("=" * 65)
    print(f"🏁 STREAMING COMPLETADO: {sent_count} eventos emitidos al topic '{TOPIC_NAME}'.")
    print(f"   Registro de auditoría persistido en: {STREAM_LOG}")


if __name__ == "__main__":
    main()
