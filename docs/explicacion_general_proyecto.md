# 🌳 Guía Descriptiva del Proyecto: Sistema de Monitoreo y Predicción de Deforestación

Este documento explica de forma clara, amena y conceptual cómo funciona este sistema de datos, qué problema resuelve en el mundo real, para qué sirve cada componente y cómo se conectan entre sí.

---

## 1. ¿Cuál es el problema real que resolvemos?

Imagina que eres la autoridad ambiental en **Bolivia** o **Colombia** encargada de proteger la Amazonía y los bosques tropicales.

Todos los días, satélites espaciales monitorean el territorio y detectan miles de puntos donde los árboles están siendo talados o quemados (alertas de deforestación). Sin embargo, te enfrentas a dos retos críticos:
1. **Recursos limitados:** No tienes suficiente personal, patrullas ni helicópteros para verificar físicamente miles de alertas al día.
2. **No toda la pérdida forestal tiene el mismo impacto ecológico:**
   - Si se tala o quema un rastrojo o pastizal que ya era usado para agricultura (**bosque secundario**), es un evento moderado.
   - Pero si se destruye **bosque primario intacto** (selva virgen con árboles milenarios, fauna endémica y gigantescos almacenes de carbono), la pérdida es **irreversible**.

El propósito de este proyecto es construir una solución completa que:
- **Centralice y certifique la calidad de los datos** satelitales, económicos y territoriales.
- **Prediga en tiempo real** cuáles alertas amenazan bosque primario virgen para enviar patrullas de inmediato.
- **Guarde un registro histórico confiable** para que los gobiernos analicen el impacto de la agricultura, la soya y la ganadería a lo largo de los años.

---

## 2. Mapa Mental de la Arquitectura

Podemos imaginar esta solución como una gran fábrica con **dos líneas de trabajo complementarias**:

```
[Satélites y Datos Públicos (GFW + Banco Mundial + FAO + GeoNames)]
                                 │
                                 ▼
                     [Bodega Segura (AWS S3)]
                                 │
                                 ▼
            [Portero de Seguridad (Great Expectations)]
                                 │
            ┌────────────────────┴────────────────────┐
            ▼                                         ▼
   [LÍNEA ANALÍTICA / HISTÓRICA]             [LÍNEA OPERACIONAL EN VIVO]
   • Orquestador: Apache Airflow             • Cinta Transportadora: Apache Kafka
   • Gran Biblioteca: PostgreSQL             • Clasificador de IA: Random Forest
   • Producto: Reporte Estático              • Producto: Radar en Vivo (Streamlit)
```

---

## 3. ¿Para qué se usa cada componente y cómo funciona?

### 🛰️ A. Las 4 Fuentes de Datos (La Materia Prima)
Ningún dato aislado cuenta la historia completa, por eso unimos 4 piezas:
1. **Global Forest Watch (GFW):** Satélites que reportan la fecha, latitud, longitud y qué tan tupidos estaban los árboles en cada alerta.
2. **Banco Mundial (World Bank):** Nos da el contexto económico del país (Producto Interno Bruto, % de tierra agrícola y población rural).
3. **FAOSTAT (Naciones Unidas para la Alimentación):** Registra cuántas toneladas de soya y cabezas de ganado se producen por año.
4. **GeoNames:** Base de datos geográfica con más de 61.000 localidades, pueblos y caseríos en Bolivia y Colombia. Con esto calculamos a cuántos kilómetros de una comunidad ocurrió la deforestación (porque casi siempre la tala ilegal avanza desde los poblados y las carreteras).

---

### 🪣 B. Amazon S3 (La Bodega Segura / Bronze Layer)
Antes de transformar o limpiar cualquier información, guardamos una copia exacta y original en la nube de Amazon (S3). Esto asegura que la "materia prima" nunca se pierda ni se altere, cumpliendo con las mejores prácticas de la arquitectura de datos moderna (*Medallion Architecture*).

---

### 🛡️ C. Great Expectations (El Portero de Seguridad)
Es una aduana automática que inspecciona el 100% de los datos antes de dejarlos pasar:
- ¿Un archivo se descargó vacío por una caída de internet? **Lo detiene de inmediato.**
- ¿Una coordenada marca una latitud fuera de los límites del planeta? **Lo detiene.**
- ¿Faltan datos obligatorios como fechas o identificadores? **Lo detiene.**
Solo si todos los datasets superan las pruebas con éxito, el sistema autoriza continuar.

---

### 🌪️ D. Apache Airflow (El Director de Orquesta)
Es el encargado de coordinar que todas las tareas se ejecuten solas, en el orden correcto y a la hora programada (todas las noches a las 10:00 PM):
1. *"Descarguen los datos del día."*
2. *"Guárdenlos en la bodega S3."*
3. *"Pásenlos por el portero de calidad de Great Expectations."*
4. *"Calculen las distancias a pueblos y limpien inconsistencias."*
5. *"Cárguenlos en la base de datos de PostgreSQL."*
Si algún paso falla, Airflow detiene el proceso y emite una alerta para evitar corromper los análisis.

---

### 🐘 E. Base de Datos PostgreSQL (La Gran Biblioteca Histórica)
Almacena los datos limpios en un **Esquema en Estrella (Star Schema)**. Está pensada para hacer consultas históricas rápidas y responder preguntas como:
- *¿Cuánta deforestación ocurrió en los años de mayor auge de la soya en Santa Cruz?*
- *¿En qué departamentos de Colombia la deforestación está más cerca de las áreas protegidas?*

---

### 🧠 F. El Modelo de Inteligencia Artificial (Machine Learning)
Entrenamos un modelo predictivo (*Random Forest*) con cientos de miles de alertas históricas:
- **¿Qué hace?** Recibe las coordenadas geográficas, la densidad de cobertura arbórea, la época del año y la distancia al pueblo más cercano, y responde una pregunta vital: **¿Esta alerta está ocurriendo sobre un bosque primario intacto?**
- **Efectividad:** Logró un **92.8% de precisión** y un **ROC-AUC de 0.9789**, lo que significa que casi nunca confunde una tala crítica en selva virgen con un desmonte agrícola ordinario.
- **Serialización:** El modelo quedó "empaquetado" en un archivo (`models/deforestation_forest_risk_pipeline.joblib`) para que pueda usarse instantáneamente en cualquier servidor sin necesidad de volver a entrenarlo cada vez.

---

### 📡 G. ¿Qué hace exactamente Apache Kafka? (La Cinta Transportadora Rápida)
Normalmente, una base de datos espera pasivamente a que alguien haga una consulta. Pero ante un incendio o una tala ilegal, **los minutos cuentan**.

Aquí es donde entra **Apache Kafka**:
1. **El Productor:** En cuanto se detecta una alerta satelital, toma la información geográfica y contextual (sin hacer trampa, es decir, sin decirle al modelo la respuesta) y la publica como un evento en un canal llamado `deforestation-alerts-stream`.
2. **El Consumidor:** Es un programa que está escuchando ese canal 24/7. En cuanto llega un evento:
   - Lo toma en milisegundos.
   - Se lo muestra al modelo de Inteligencia Artificial empaquetado.
   - El modelo calcula la probabilidad en **menos de 15 milisegundos**.
   - Si la probabilidad supera el umbral crítico, el consumidor clasifica el evento como **🚨 CRÍTICO (Bosque Primario)** y lo envía de inmediato a la pantalla del operador.

---

### 🖥️ H. Streamlit (La Pantalla de Radar en Tiempo Real)
Es la interfaz web para los guardaparques y autoridades en campo:
- **Mapa interactivo en 3D (Pydeck):** Las alertas críticas en bosque primario se iluminan en **rojo brillante**, mientras que las de bosque secundario aparecen en **amarillo**.
- **Feed en vivo:** Muestra las últimas alertas que han llegado por Kafka, su distancia al pueblo más cercano y la certeza que tiene la IA.
- **Métricas inmediatas:** Conteo de alertas procesadas, porcentaje de riesgo crítico y velocidad de llegada.

---

### 📄 I. El Reporte Estático en Python (El Informe Estratégico)
Es el producto para ministros, científicos y tomadores de decisiones:
- Se genera con un solo comando (`python scripts/generate_static_report.py`).
- No depende de que Kafka esté prendido; consulta directamente los datos históricos validados.
- Contiene gráficos comparativos entre Bolivia y Colombia, análisis de los factores económicos que impulsan la tala (ganadería y monocultivos) y cálculos de emisiones de CO₂e según las normas del IPCC.

---

## 4. El Concepto del "Doble Valor" de la Arquitectura

La gran ventaja de este diseño es que una sola infraestructura satisface dos necesidades que antes estaban desconectadas:

| Dimensión | ⚡ Ruta Operacional (Tiempo Real) | 📊 Ruta Analítica (Histórica) |
| :--- | :--- | :--- |
| **Audiencia** | Guardaparques, patrullas terrestres y brigadistas. | Ministros de ambiente, investigadores y directores de política pública. |
| **Tecnologías** | **Kafka + Modelo ML Serializado + Streamlit** | **Airflow + Great Expectations + PostgreSQL + Reporte Python** |
| **Tiempo de Respuesta** | **Milisegundos (< 15 ms por alerta)** | **Lotes diarios (Batch a las 22:00)** |
| **Pregunta que Responde** | *¿A dónde debemos mandar la patrulla hoy mismo para salvar selva virgen?* | *¿Cuánto carbono emitimos en los últimos 4 años y qué leyes debemos cambiar?* |
