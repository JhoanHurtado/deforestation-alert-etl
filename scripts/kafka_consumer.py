#!/usr/bin/env python3
"""
kafka_consumer.py — Consumidor de Kafka para inferencia de Machine Learning en tiempo real.

Requisitos de la Entrega Final (Kafka Consumer and Inference):
  1. Carga el pipeline serializado 'models/deforestation_forest_risk_pipeline.joblib'.
  2. Consume eventos del topic 'deforestation-alerts-stream' en tiempo real (o del stream local persistente).
  3. Ejecuta inferencia continua ('predict' y 'predict_proba') para cada mensaje entrante.
  4. Genera predicción de riesgo:
     - 1: Riesgo Crítico — Pérdida en Bosque Primario (Intacto)
     - 0: Pérdida en Bosque Secundario / Rastrojo
  5. Publica o guarda los resultados enriquecidos con su predicción y probabilidad
     en 'data/processed/realtime_predictions.jsonl' para el dashboard de visualización.
"""

import sys
import os
import json
import time
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
import joblib

PROJECT_ROOT = Path(__file__).parent.parent
MODEL_PATH   = PROJECT_ROOT / "models" / "deforestation_forest_risk_pipeline.joblib"
STREAM_LOG   = PROJECT_ROOT / "data" / "logs" / "kafka_stream_events.jsonl"
OUTPUT_PREDS = PROJECT_ROOT / "data" / "processed" / "realtime_predictions.jsonl"
OUTPUT_PREDS.parent.mkdir(parents=True, exist_ok=True)

TOPIC_NAME = "deforestation-alerts-stream"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def get_consumer():
    """Intenta inicializar el consumidor de Kafka real."""
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=5000,
        )
        print(f"✅ Conectado a Kafka Consumer en {BOOTSTRAP_SERVERS} (Topic: {TOPIC_NAME})")
        return consumer
    except Exception as e:
        print(f"⚠️ No se pudo conectar al consumidor Kafka real ({e}).")
        print("  ▶ Leyendo eventos del bus de streaming local persistente.")
        return None


def main():
    parser = argparse.ArgumentParser(description="Consumidor Kafka con Inferencia ML")
    parser.add_argument("--max-events", type=int, default=50, help="Número máximo de eventos a procesar (default: 50)")
    parser.add_argument("--follow", action="store_true", help="Seguir consumiendo indefinidamente (tail -f)")
    args = parser.parse_args()

    print("=" * 65)
    print("🤖 INICIANDO CONSUMIDOR KAFKA & INFERENCIA EN TIEMPO REAL")
    print(f"   Modelo: {MODEL_PATH.name} | Topic: {TOPIC_NAME}")
    print("=" * 65)

    if not MODEL_PATH.exists():
        print(f"❌ Error: El modelo {MODEL_PATH} no existe. Ejecuta scripts/train_model.py primero.")
        sys.exit(1)

    print(f"▶ Cargando pipeline de Machine Learning...")
    pipeline = joblib.load(MODEL_PATH)
    print("  ✅ Pipeline cargado en memoria.")

    consumer = get_consumer()
    processed_count = 0

    def process_event(event: dict):
        nonlocal processed_count
        # Preparar DataFrame de una sola fila para el pipeline de inferencia
        row_df = pd.DataFrame([{
            "latitude": float(event["latitude"]),
            "longitude": float(event["longitude"]),
            "tree_cover_density_pct": float(event.get("tree_cover_density_pct", 50.0)),
            "dist_nearest_place_km": float(event.get("dist_nearest_place_km", 10.0)),
            "month": int(event.get("month", 1)),
            "week": int(event.get("week", 1)),
            "country_code": str(event.get("country_code", "COL")),
            "driver_id": int(event.get("driver_id", 7)),
        }])

        pred = int(pipeline.predict(row_df)[0])
        proba = float(pipeline.predict_proba(row_df)[0, 1])

        result = {
            **event,
            "prediction": pred,
            "prediction_label": "CRÍTICO (Bosque Primario)" if pred == 1 else "MODERADO (Bosque Secundario)",
            "primary_forest_probability": round(proba, 4),
            "inference_timestamp": datetime.now().isoformat(),
        }

        with open(OUTPUT_PREDS, "a", encoding="utf-8") as f_out:
            f_out.write(json.dumps(result) + "\n")

        processed_count += 1
        indicator = "🚨 [CRÍTICO]" if pred == 1 else "🟡 [MODERADO]"
        print(f"  {indicator} #{processed_count} Evt: {event.get('event_id', 'N/A')} | Prob: {proba:.1%} | Pred: {result['prediction_label']}")

    # Modo 1: Consumidor Kafka Real
    if consumer:
        print("▶ Escuchando mensajes desde broker Kafka...")
        for message in consumer:
            process_event(message.value)
            if not args.follow and processed_count >= args.max_events:
                break

    # Modo 2: Consumidor Stream Local Persistente (Fallback)
    else:
        if not STREAM_LOG.exists():
            print(f"⚠️ No hay archivo de stream local en {STREAM_LOG}. Ejecuta primero scripts/kafka_producer.py.")
            sys.exit(0)

        print(f"▶ Procesando eventos registrados en {STREAM_LOG.name}...")
        with open(STREAM_LOG, "r", encoding="utf-8") as f_in:
            for line in f_in:
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                    process_event(event)
                    if not args.follow and processed_count >= args.max_events:
                        break
                except Exception as e:
                    print(f"  ❌ Error procesando línea: {e}")

    print("=" * 65)
    print(f"🏁 INFERENCIA FINALIZADA: {processed_count} eventos clasificados.")
    print(f"   Predicciones listas para el Dashboard en: {OUTPUT_PREDS}")


if __name__ == "__main__":
    main()
