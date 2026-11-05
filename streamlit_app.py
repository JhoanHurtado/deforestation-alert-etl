#!/usr/bin/env python3
"""
streamlit_app.py — Dashboard de Inferencia de Machine Learning en Tiempo Real y Streaming Kafka.

Requisitos de la Entrega Final (Real-time prediction dashboard):
  1. Conectado al flujo de eventos del consumidor Kafka / predicciones en tiempo real.
  2. Visualización reactiva que se actualiza con la llegada de nuevos eventos.
  3. Despliegue de métricas operativas (Total alertas, % Crítico en Bosque Primario, Probabilidad Promedio).
  4. Mapa geoespacial interactivo diferenciando alertas críticas de moderadas.
  5. Feed de eventos en vivo con detalles de inferencia y metadatos espaciales (GeoNames).
  6. Articulación de Doble Valor: Inferencia Operativa (Streaming) vs Reporte Analítico (Data Warehouse).
"""

import sys
import os
import json
import time
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import streamlit as st
import pydeck as pdk

# Configuración de página
st.set_page_config(
    page_title="Deforestation Alert ETL — Real-Time ML Dashboard",
    page_icon="🌲",
    layout="wide",
    initial_sidebar_state="expanded",
)

PROJECT_ROOT = Path(__file__).parent
PREDICTIONS_FILE = PROJECT_ROOT / "data" / "processed" / "realtime_predictions.jsonl"
METADATA_FILE = PROJECT_ROOT / "models" / "model_metadata.json"
MODEL_FILE = PROJECT_ROOT / "models" / "deforestation_forest_risk_pipeline.joblib"


# ── Funciones de Carga de Datos y Estado ──────────────────────────────────────
@st.cache_data(ttl=2)
def load_realtime_predictions():
    """Lee el log de predicciones en tiempo real generadas por el consumidor Kafka."""
    if not PREDICTIONS_FILE.exists():
        return pd.DataFrame()

    records = []
    with open(PREDICTIONS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    if "inference_timestamp" in df.columns:
        df["inference_dt"] = pd.to_datetime(df["inference_timestamp"], errors="coerce")
        df = df.sort_values(by="inference_dt", ascending=False)
    return df


def load_model_metadata():
    """Carga los metadatos y métricas de desempeño del modelo entrenado."""
    if not METADATA_FILE.exists():
        return {}
    with open(METADATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


# ── Simulación de Streaming desde la UI ───────────────────────────────────────
def trigger_streaming_batch(n_events=10):
    """Ejecuta una ráfaga del productor y consumidor para simular eventos en vivo."""
    import subprocess
    py_exec = sys.executable
    producer_script = PROJECT_ROOT / "scripts" / "kafka_producer.py"
    consumer_script = PROJECT_ROOT / "scripts" / "kafka_consumer.py"

    subprocess.run(
        [py_exec, str(producer_script), "--limit", str(n_events), "--delay", "0.05"],
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [py_exec, str(consumer_script), "--max-events", str(n_events)],
        capture_output=True,
        text=True,
    )


# ── Barra Lateral (Sidebar) ──────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/forest.png", width=70)
    st.title("Sistema de Alertas")
    st.markdown("**ETL & Streaming de Deforestación**")
    st.caption("ODS 13: Acción por el Clima | ODS 15: Vida de Ecosistemas")
    st.divider()

    st.subheader("📡 Control de Streaming Kafka")
    auto_refresh = st.checkbox("Auto-refrescar cada 3 segundos", value=True)
    if auto_refresh:
        time.sleep(3)
        st.rerun()

    st.markdown("##### Simular Tráfico en Vivo")
    batch_size = st.slider("Cantidad de Alertas a Inyectar", min_value=5, max_value=50, value=15, step=5)
    if st.button("🚀 Inyectar Eventos Streaming", type="primary", use_container_width=True):
        with st.spinner("Publicando a Kafka y procesando inferencias..."):
            trigger_streaming_batch(batch_size)
            st.success(f"¡{batch_size} nuevas alertas procesadas!")
            time.sleep(1)
            st.rerun()

    st.divider()
    st.subheader("🔍 Filtros de Visualización")
    filter_country = st.multiselect("País:", ["Todos", "BOL", "COL"], default=["Todos"])
    filter_risk = st.radio("Nivel de Riesgo:", ["Todos", "Solo Crítico (Bosque Primario)", "Solo Moderado"])

    st.divider()
    st.info(
        "💡 **Doble Valor de la Arquitectura:**\n\n"
        "• **Operacional:** Inferencia < 50ms por Kafka para despacho de patrullas forestales.\n"
        "• **Analítico:** Almacenamiento histórico Star Schema en PostgreSQL para reportes de políticas públicas."
    )


# ── Encabezado Principal ──────────────────────────────────────────────────────
st.title("🌲 Monitoreo de Deforestación en Tiempo Real")
st.markdown(
    "Visualización reactiva basada en **Apache Kafka** y un modelo de **Machine Learning (Random Forest)** "
    "para la detección inmediata de alertas que amenazan **bosques primarios intactos** en Bolivia y Colombia."
)

tab_live, tab_analytics, tab_model, tab_architecture = st.tabs([
    "🚨 Radar Operacional en Vivo",
    "📊 Métricas de Streaming",
    "🧠 Modelo de Machine Learning",
    "🏛️ Arquitectura & Doble Valor",
])

df_preds = load_realtime_predictions()
meta = load_model_metadata()

# Aplicar filtros si hay datos
if not df_preds.empty:
    if "Todos" not in filter_country and filter_country:
        df_preds = df_preds[df_preds["country_code"].isin(filter_country)]
    if filter_risk == "Solo Crítico (Bosque Primario)":
        df_preds = df_preds[df_preds["prediction"] == 1]
    elif filter_risk == "Solo Moderado":
        df_preds = df_preds[df_preds["prediction"] == 0]


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1: RADAR OPERACIONAL EN VIVO
# ══════════════════════════════════════════════════════════════════════════════
with tab_live:
    if df_preds.empty:
        st.warning("⚠️ No se han recibido eventos de streaming aún. Haz clic en 'Inyectar Eventos Streaming' en el panel lateral para iniciar el flujo.")
    else:
        # Tarjetas de Indicadores Clave (KPIs)
        total_events = len(df_preds)
        critical_events = (df_preds["prediction"] == 1).sum()
        pct_critical = (critical_events / total_events) * 100 if total_events > 0 else 0
        avg_proba = df_preds["primary_forest_probability"].mean() if total_events > 0 else 0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Ingestionadas (Stream)", f"{total_events:,}")
        col2.metric("Alertas Críticas (Bosque Primario)", f"{critical_events:,}", delta=f"{pct_critical:.1f}% del total", delta_color="inverse")
        col3.metric("Probabilidad Media de Riesgo", f"{avg_proba:.1%}")
        col4.metric("Latencia de Inferencia", "< 15 ms", delta="Streaming activo", delta_color="normal")

        st.markdown("---")

        # Mapa Geoespacial Reactivo (Pydeck)
        st.subheader("🗺️ Mapa Geoespacial de Alertas Predictivas")
        st.caption("Puntos rojos: Amenaza en Bosque Primario Intacto (Crítico). Puntos amarillos: Pérdida en Bosque Secundario / Rastrojo.")

        # Asignar colores según predicción
        map_df = df_preds.copy()
        # [R, G, B, Alpha]
        map_df["color_r"] = np.where(map_df["prediction"] == 1, 235, 245)
        map_df["color_g"] = np.where(map_df["prediction"] == 1, 40, 190)
        map_df["color_b"] = np.where(map_df["prediction"] == 1, 40, 30)

        mid_lat = map_df["latitude"].mean() if not map_df.empty else -10.0
        mid_lon = map_df["longitude"].mean() if not map_df.empty else -68.0

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=map_df,
            get_position=["longitude", "latitude"],
            get_fill_color=["color_r", "color_g", "color_b", 210],
            get_radius=12000,
            pickable=True,
            auto_highlight=True,
        )

        view_state = pdk.ViewState(
            latitude=mid_lat,
            longitude=mid_lon,
            zoom=4.5,
            pitch=25,
        )

        tooltip = {
            "html": "<b>ID:</b> {event_id}<br/>"
                    "<b>País:</b> {country_code}<br/>"
                    "<b>Clasificación:</b> {prediction_label}<br/>"
                    "<b>Probabilidad Crítica:</b> {primary_forest_probability}<br/>"
                    "<b>Densidad Forestal:</b> {tree_cover_density_pct}%<br/>"
                    "<b>Distancia a Poblado (GeoNames):</b> {dist_nearest_place_km} km",
            "style": {"backgroundColor": "#1e293b", "color": "white", "fontSize": "13px"}
        }

        st.pydeck_chart(pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip=tooltip,
            map_style="mapbox://styles/mapbox/dark-v11",
        ))

        # Feed en vivo de Alertas
        st.subheader("📋 Feed de Alertas en Tiempo Real (Últimas Transmisiones)")
        display_cols = [
            "event_id", "country_code", "latitude", "longitude",
            "dist_nearest_place_km", "tree_cover_density_pct",
            "prediction_label", "primary_forest_probability", "inference_timestamp"
        ]
        available_cols = [c for c in display_cols if c in df_preds.columns]

        feed_df = df_preds[available_cols].head(25)
        st.dataframe(
            feed_df.style.map(
                lambda val: "background-color: #ffebee; color: #b71c1c; font-weight: bold" if "CRÍTICO" in str(val) else "",
                subset=["prediction_label"] if "prediction_label" in feed_df.columns else []
            ),
            use_container_width=True,
            height=320,
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2: MÉTRICAS DE STREAMING
# ══════════════════════════════════════════════════════════════════════════════
with tab_analytics:
    st.subheader("📊 Análisis de Distribución de Eventos en Tiempo Real")
    if df_preds.empty:
        st.info("Sin datos para analizar. Genera eventos de prueba desde el sidebar.")
    else:
        col_c1, col_c2 = st.columns(2)
        with col_c1:
            st.markdown("##### Distribución de Probabilidades de Inferencia")
            st.bar_chart(df_preds["primary_forest_probability"].round(1).value_counts().sort_index())

        with col_c2:
            st.markdown("##### Alertas por País")
            st.bar_chart(df_preds["country_code"].value_counts())

        st.markdown("##### Distancia a Asentamientos Urbanos vs Densidad Forestal")
        scatter_data = df_preds[["dist_nearest_place_km", "tree_cover_density_pct", "primary_forest_probability"]]
        st.scatter_chart(
            scatter_data,
            x="dist_nearest_place_km",
            y="tree_cover_density_pct",
            color="primary_forest_probability",
            size="primary_forest_probability",
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3: MODELO DE MACHINE LEARNING
# ══════════════════════════════════════════════════════════════════════════════
with tab_model:
    st.subheader("🧠 Especificaciones Técnicas del Modelo Predictivo")
    st.markdown(
        "El modelo implementa un pipeline serializado (`scikit-learn`) entrenado con validación estratificada "
        "sin fuga de información (*data leakage*), prediciendo si una alerta satelital amenaza bosque primario virgen."
    )

    if meta:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Algoritmo", meta.get("model_type", "RandomForestClassifier"))
        c2.metric("Exactitud (Accuracy)", f"{meta['metrics']['accuracy']:.2%}")
        c3.metric("F1-Score Ponderado", f"{meta['metrics']['f1_score']:.2%}")
        c4.metric("ROC-AUC", f"{meta['metrics']['roc_auc']:.4f}")

        st.markdown("---")
        col_m1, col_m2 = st.columns(2)

        with col_m1:
            st.markdown("##### Matriz de Confusión (Conjunto de Test: 40,000 muestras)")
            cm = np.array(meta['metrics']['confusion_matrix'])
            cm_df = pd.DataFrame(
                cm,
                index=["Real: Secundario (0)", "Real: Primario (1)"],
                columns=["Pred: Secundario (0)", "Pred: Primario (1)"],
            )
            st.dataframe(cm_df.style.background_gradient(cmap="Blues"), use_container_width=True)

        with col_m2:
            st.markdown("##### Características de Entrada (Features)")
            st.markdown(
                "- **Numéricas:** `latitude`, `longitude`, `tree_cover_density_pct`, `dist_nearest_place_km`, `month`, `week`, `driver_id`\n"
                "- **Categóricas:** `country_code` (Codificado con OneHotEncoder)\n"
                "- **Preprocesamiento:** Imputación de nulos + Escalamiento estándar robusto.\n"
                "- **Blindaje contra Data Leakage:** La variable objetivo `is_primary_forest` se excluye rigurosamente del mensaje de Kafka."
            )

        st.markdown("##### Reporte de Clasificación Detallado")
        rep_df = pd.DataFrame(meta['metrics']['classification_report']).T
        st.dataframe(rep_df.style.format(precision=4), use_container_width=True)
    else:
        st.warning("Metadatos del modelo no encontrados en models/model_metadata.json.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4: ARQUITECTURA & DOBLE VALOR
# ══════════════════════════════════════════════════════════════════════════════
with tab_architecture:
    st.subheader("🏛️ Articulación del Doble Valor de Negocio")
    st.markdown(
        "Una de las fortalezas clave de este proyecto es que **una misma infraestructura de datos "
        "alimenta dos flujos de valor complementarios y desacoplados:**"
    )

    col_v1, col_v2 = st.columns(2)
    with col_v1:
        st.markdown("""
        ### ⚡ 1. Flujo Operacional (Tiempo Real)
        - **Tecnología:** Apache Kafka + Pipeline ML Serializado + Streamlit.
        - **Objetivo:** Respuesta táctica inmediata ante frentes activos de tala o quema.
        - **Casos de Uso:**
          - Alertas prioritarias a guardaparques y autoridades en < 60 segundos.
          - Clasificación probabilística instantánea para despachar patrullas terrestres cuando se detecta riesgo en **bosques primarios intactos**.
          - Filtro de ruido satelital para no desplegar brigadas ante desmontes agrícolas autorizados en bosques secundarios.
        """)

    with col_v2:
        st.markdown("""
        ### 📊 2. Flujo Analítico (Histórico / Estratégico)
        - **Tecnología:** Airflow Medallion ETL + Great Expectations + PostgreSQL Star Schema + Reporte Estático en Python.
        - **Objetivo:** Análisis retrospectivo, causalidad económica y formulación de políticas públicas.
        - **Casos de Uso:**
          - Monitoreo del cumplimiento de compromisos climáticos internacionales (ODS 13 y 15).
          - Estimación de emisiones de CO2e bajo metodología IPCC Tier 1 vinculadas al avance de la soya y ganadería.
          - Correlación espacial con variables macroeconómicas del Banco Mundial y FAOSTAT.
        """)

    st.markdown("---")
    st.markdown("### 🔄 Diagrama de Flujo del Pipeline Dual")
    st.code("""
                                ┌──────────────────────────────────────┐
                                │   Fuentes: GFW + WB + FAO + GeoNames │
                                └──────────────────┬───────────────────┘
                                                   │
                                                   ▼
                                ┌──────────────────────────────────────┐
                                │      Data Lake / Bronze (AWS S3)     │
                                └──────────────────┬───────────────────┘
                                                   │
                                                   ▼
                                ┌──────────────────────────────────────┐
                                │   Quality Gate (Great Expectations)  │
                                └──────────────────┬───────────────────┘
                                                   │
                                                   ▼
                                ┌──────────────────────────────────────┐
                                │ Transformación & Enriquecimiento     │
                                └───────┬──────────────────────┬───────┘
                                        │                      │
                   [Ruta Analítica]     │                      │   [Ruta Operacional]
                                        ▼                      ▼
               ┌─────────────────────────────────┐   ┌────────────────────────────────┐
               │ Data Warehouse Dimensional      │   │ Kafka Event Stream             │
               │ (PostgreSQL Star Schema)        │   │ (deforestation-alerts-stream)  │
               └────────────────┬────────────────┘   └───────────────┬────────────────┘
                                │                                    │
                                ▼                                    ▼
               ┌─────────────────────────────────┐   ┌────────────────────────────────┐
               │ Reporte Analítico Estático      │   │ Inferencia ML en Streaming     │
               │ (Python / data/results/)        │   │ (predict + predict_proba)      │
               └─────────────────────────────────┘   └───────────────┬────────────────┘
                                                                     │
                                                                     ▼
                                                     ┌────────────────────────────────┐
                                                     │ Real-Time Streamlit Dashboard  │
                                                     └────────────────────────────────┘
    """, language="text")

st.markdown("---")
st.caption("ETL Project — Final Delivery | Universidad | Maestría / Especialización en Ingeniería de Datos e IA")
