# 🎤 Presentation Guide, Speaker Notes & Live Demo Script
## Deforestation Alert ETL & Real-Time ML Prediction System
**Course:** ETL (G51) • Faculty of Engineering and Basic Sciences  
**Allocated Presentation Time:** 10 to 15 minutes  
**Target:** 100% English Presentation (+0.5 Grade Bonus)

---

## 📋 Presentation Overview & Timing Breakdown

| Slide # | Slide Title | Key Rubric Criteria Addressed | Target Timing | Cumulative Time |
|:---:|:---|:---|:---:|:---:|
| **1** | Title & Introduction | Project Context & Scope | 1:00 min | 0:00 - 1:00 |
| **2** | Project Evolution & Timeline | Evolution & Adaptation from Feedback | 1:30 min | 1:00 - 2:30 |
| **3** | End-to-End System Architecture | Dual-Value Engineering Design | 1:30 min | 2:30 - 4:00 |
| **4** | Data Quality Gate & Airflow DAG | Zero-Tolerance GX & Atomic Orchestration | 1:30 min | 4:00 - 5:30 |
| **5** | Dimensional DWH: Star Schema | OLAP Relational Design & Normalization | 1:30 min | 5:30 - 7:00 |
| **6** | Machine Learning: Primary Forest Risk | Predictive Formulation, Metrics & No Leakage | 1:30 min | 7:00 - 8:30 |
| **7** | Kafka Event Streaming Architecture | Event Schema, Topic Design & Latency | 1:15 min | 8:30 - 9:45 |
| **8** | Dual-Value Generation in Action | Analytical Reporting vs. Real-Time Dispatch | 1:15 min | 9:45 - 11:00 |
| **9** | Technical Challenges & Breakthroughs | Problem-Solving (Airflow 3, BallTree, Drift) | 1:15 min | 11:00 - 12:15 |
| **10** | Live Demonstration Roadmap | Analytical & Operational Live Proof | 1:30 min | 12:15 - 13:45 |
| **11** | Global Impact & SDG Alignment | UN SDGs 13 & 15, EUDR, Carbon Accounting | 0:45 min | 13:45 - 14:30 |
| **12** | Conclusion & Q&A | Summary & Open Discussion | 0:30 min | 14:30 - 15:00 |

---

## 🗣️ Slide-by-Slide English Speech Script (Speaker Notes)

### Slide 1: Title Slide
- **Slide Title:** Deforestation Alert ETL & Real-Time ML System
- **Subtitle:** A Dual-Value Architecture: Analytical Dimensional Warehouse & Operational Streaming Inferences
- **Speaker Script (Read aloud in English):**
> *"Good morning, professor and fellow colleagues. Today, our group presents the final defense of our engineering project: 'Deforestation Alert ETL and Real-Time Machine Learning System for Bolivia and Colombia'.  
> Over the semester, our primary objective was to design a production-grade data architecture capable of tackling one of South America's most critical environmental emergencies: tropical forest destruction.  
> Crucially, our architecture demonstrates how a single, rigorously validated data foundation can simultaneously deliver **dual business value**: strategic retrospective reporting through an Airflow-orchestrated dimensional warehouse, and tactical, sub-second operational interventions powered by Apache Kafka and Machine Learning. Let us guide you through our evolution, technical decisions, and live platform."*

---

### Slide 2: Project Evolution & Timeline
- **Slide Title:** Project Evolution & Milestone Timeline
- **Category:** Project Roadmap & Adaptation
- **Speaker Script:**
> *"To truly appreciate our engineering decisions, we must look at how this project evolved across three distinct milestones.  
> In **Phase 1**, we began with a single raw dataset from Global Forest Watch. While we achieved baseline exploratory analysis, we realized that standalone satellite coordinates could not explain the socio-economic drivers behind deforestation.  
> In **Phase 2**, we integrated three complementary sources: macroeconomic indicators from the World Bank, crop production from FAOSTAT, and over 61,000 populated places from GeoNames. More importantly, we received critical academic feedback: our initial quality check only sampled 10,000 rows, our ETL task was monolithic, and naive merges risked data explosion. We listened and resolved every point: we expanded Great Expectations to audit 100% of the 1.5 million records, restructured the Airflow DAG into atomic Medallion tasks, and replaced unspaced joins with spherical BallTree spatial indexing.  
> Finally, in our **Final Delivery**, we introduced real-time streaming with Apache Kafka, zero-leakage ML inference, and dual consumption products."*

---

### Slide 3: End-to-End System Architecture
- **Slide Title:** End-to-End System Architecture
- **Category:** Dual-Value Engineering Design
- **Speaker Script:**
> *"Slide 3 illustrates our complete system architecture. As depicted in the diagram, our pipeline strictly adheres to the modern Medallion data architecture.  
> First, multi-source ingestion tasks fetch satellite alerts, World Bank APIs, FAO data dumps, and GeoNames. All raw files are immutably persisted into our **Amazon S3 Bronze Data Lake** before any manipulation, guaranteeing complete auditability.  
> Next, data must pass through our **Great Expectations Quality Gate**. If any anomalies or empty datasets are detected, execution is immediately halted.  
> In the Silver Layer, clean data is enriched with geodesic distance calculations. From this point, the pipeline branches into two decoupled streams:  
> **Path A** loads into our PostgreSQL Star Schema for static analytical reporting and IPCC carbon modeling.  
> **Path B** publishes stripped feature vectors into Apache Kafka, where a continuous consumer executes Machine Learning inferences with sub-15 millisecond latency."*

---

### Slide 4: Data Quality Gate & Atomic Airflow Orchestration
- **Slide Title:** Data Quality Gate & Atomic Airflow Orchestration
- **Category:** Robustness & Reproducibility
- **Speaker Script:**
> *"On Slide 4, we examine our orchestration and automated quality gates.  
> Addressing previous critiques, we eliminated all artificial row-subsampling. Our Great Expectations suite now verifies 100% of our 1.52 million rows in under three seconds. It enforces geographic boundaries, value sets, and crucially, zero tolerance for empty files: if an API returns zero bytes, the gate triggers an exit code 1, aborting downstream loads.  
> In Apache Airflow, we refactored the workflow into six atomic BashOperators scheduled nightly at 22:00 UTC. Furthermore, we resolved the Airflow 3 authentication migration by configuring the new SimpleAuthManager with role-based JWT security."*

---

### Slide 5: Dimensional Data Warehouse (Star Schema)
- **Slide Title:** Dimensional Data Warehouse: Star Schema Design
- **Category:** Analytics Data Modeling
- **Speaker Script:**
> *"Slide 5 showcases our dimensional model in PostgreSQL.  
> At the center lies **`fact_alerts`**, where each record represents a single satellite pixel detection. Each alert references surrogate keys from `dim_date`, `dim_location`, `dim_driver`, `dim_land_cover`, `dim_confidence`, and `dim_economic_context`.  
> Notice a key architectural decision: macroeconomic indicators (such as national GDP or soy yields) change yearly, not per alert. By normalizing this into `dim_economic_context`, we avoided repeating macro series across 1.5 million rows, preventing Cartesian duplication.  
> We also populated `dim_places` with 61,749 populated settlements. With B-Tree indexes across all foreign keys, analytical OLAP queries execute in milliseconds."*

---

### Slide 6: Machine Learning: Predicting Primary Forest Risk
- **Slide Title:** Machine Learning: Predicting Primary Forest Risk
- **Category:** Predictive Intelligence
- **Speaker Script:**
> *"Moving to Slide 6, we formulated a high-impact Machine Learning problem:  
> Detecting deforestation is helpful, but detecting whether an alert threatens **intact virgin primary forest** is vital. Secondary regrowth loss is manageable; primary forest destruction is ecologically irreversible.  
> To prevent data leakage, the target variable `is_primary_forest` is strictly excluded from Kafka messages. The model only receives spatial coordinates, canopy density, distance to settlements from GeoNames, month, week, and driver category.  
> Our trained Random Forest pipeline achieved **92.77% Accuracy**, a **92.71% weighted F1-score**, and an outstanding **0.9789 ROC-AUC** across a 40,000-sample test set. The model is compiled into a single joblib file, allowing instant loading."*

---

### Slide 7: Event Streaming Architecture with Apache Kafka
- **Slide Title:** Event Streaming Architecture with Apache Kafka
- **Category:** Real-Time Pipeline
- **Speaker Script:**
> *"Slide 7 covers our real-time streaming backbone.  
> Our Python Kafka Producer streams alert records into the topic **`deforestation-alerts-stream`** with simulated intervals, mimicking live satellite feeds.  
> On the receiving side, our Kafka Consumer operates 24/7. It loads the compiled Random Forest pipeline once upon startup, consumes incoming JSON events, and executes dual inference: predicting the binary class and calculating calibrated risk probabilities in under 15 milliseconds.  
> Inferences are enriched with critical vs moderate warning badges and streamed directly to our operational dashboard."*

---

### Slide 8: Dual-Value Generation: Analytical vs. Operational
- **Slide Title:** Dual-Value Generation: Analytical vs. Operational
- **Category:** Business Impact
- **Speaker Script:**
> *"Slide 8 represents the cornerstone of our final delivery: **Dual-Value Articulation**.  
> We did not implement Kafka or Machine Learning as an isolated exercise; they serve two completely different personas:  
> **Path 1 is Analytical and Strategic:** Designed for Environmental Ministers, policy makers, and climate economists. It queries our PostgreSQL Data Warehouse to produce static analytical briefs, examining annual trends, driver causality, and Tier 1 IPCC carbon emissions.  
> **Path 2 is Operational and Tactical:** Designed for Park Rangers and emergency brigades on the ground. Operating at sub-second speeds through Kafka, ML, and Streamlit, it tells rangers: 'Deploy here immediately; this alert is 96% likely to be destroying intact virgin jungle.'  
> One unified architecture answers both strategic policy questions and immediate tactical emergencies."*

---

### Slide 9: Technical Challenges & Engineering Breakthroughs
- **Slide Title:** Technical Challenges & Engineering Breakthroughs
- **Category:** Problem Solving
- **Speaker Script:**
> *"Engineering is defined by how we overcome technical bottlenecks. Slide 9 highlights three major breakthroughs:  
> 1. **Airflow 3 FastAPI Auth:** In Airflow 3, the CLI command `airflow users create` was deprecated. SimpleAuthManager initially failed JWT creation. We diagnosed that roles must be strictly uppercase (`admin:ADMIN`) and configured verified password files, restoring seamless authentication.  
> 2. **Geospatial Big Data Scale:** Merging 1.5 million alerts with 61,000 GeoNames places naively would compute 91 billion pairwise comparisons, causing out-of-memory crashes. We implemented scikit-learn's spherical `BallTree` with Haversine metrics in radian space, finishing in just 4.2 seconds with minimal memory.  
> 3. **Schema Drift:** We standardized column normalization in our Silver layer, eliminating upstream renaming bugs."*

---

### Slide 10: Live Demonstration Roadmap
- **Slide Title:** Live Demonstration Roadmap
- **Category:** System Verification
- **Speaker Script:**
> *"We will now demonstrate both operational and analytical fronts live in our environment:  
> First, in the **Analytical Front**, we will verify the Airflow DAG execution, confirm Great Expectations auditing 100% of records, and execute `scripts/generate_static_report.py` to inspect the generated Markdown report and carbon emission graphs.  
> Second, in the **Operational Front**, we will launch our Streamlit dashboard, start our Kafka consumer, inject streaming alert events, and observe the interactive 3D Pydeck map light up in real time with high-risk red alerts."*

---

### Slide 11: Global Impact: UN Sustainable Development Goals
- **Slide Title:** Global Impact: UN Sustainable Development Goals
- **Category:** Sustainability & Policy
- **Speaker Script:**
> *"Before concluding, we want to emphasize the real-world significance of this data architecture.  
> Our project directly aligns with **UN SDG 13 (Climate Action)** and **SDG 15 (Life on Land)**.  
> By translating canopy loss into IPCC Tier 1 metric tons of CO2e and correlating with FAO soy cultivation, we provide an auditable framework for international compliance, such as the European Union Deforestation Regulation.  
> Furthermore, prioritizing primary forests in real time empowers local authorities to protect indigenous reserves and endemic biodiversity."*

---

### Slide 12: Conclusion & Q&A
- **Slide Title:** Summary of Achievements
- **Speaker Script:**
> *"In conclusion, our project delivers an enterprise-grade Medallion architecture, 100% full-dataset quality gates, 92.8% accurate Machine Learning inference, and a working Kafka streaming bus with dual consumption products.  
> Thank you very much for your time and guidance throughout this course. We are now delighted to take any questions."*

---

## 🎬 2-Minute Live Demonstration Checklist

Have these 2 terminal windows and browser tabs open before presenting:

### Terminal 1: Streamlit Dashboard
```bash
source venv/bin/activate
streamlit run streamlit_app.py
```
*(Runs at `http://localhost:8501`. Have the browser tab open showing the 3D Pydeck map).*

### Terminal 2: Kafka Streaming Injection
```bash
source venv/bin/activate

# Step A: Show static report generation (takes 3 seconds)
python scripts/generate_static_report.py

# Step B: Inject 30 real-time alerts into Kafka
python scripts/kafka_producer.py --limit 30 --delay 0.05

# Step C: Consume alerts and trigger ML inference live
python scripts/kafka_consumer.py --max-events 30
```
*(Switch to browser tab at `http://localhost:8501`: watch the total counter increase, the live alert feed populate, and the red/amber markers appear on the 3D map!)*

---

## ❓ Anticipated Questions & Engineering Answers (Q&A Defense)

**Q1: Why did you choose Random Forest instead of a Deep Learning neural network?**  
- *Answer:* "Given our tabular and geospatial feature set (coordinates, density, distance, calendar week), tree-based ensembles (Random Forest / Gradient Boosting) consistently outperform neural networks on tabular data without requiring massive GPU infrastructure. Furthermore, Random Forest delivers sub-15ms inference latency, which is essential for Kafka streaming, and provides native feature importance interpretability."

**Q2: How do you prevent data leakage in the Kafka streaming pipeline?**  
- *Answer:* "Data leakage occurs when the target variable or downstream information is included in the feature set during inference. In `scripts/kafka_producer.py`, we explicitly drop `is_primary_forest` from the payload before publishing to the `deforestation-alerts-stream` topic. The Kafka consumer receives only the raw coordinates, forest density, and contextual features, forcing the model to infer the risk independently."

**Q3: How does the BallTree spatial join differ from a standard SQL join?**  
- *Answer:* "A traditional Cartesian join calculates distance between every alert ($N=1.5M$) and every town ($M=61k$), which equals 91 billion operations and causes out-of-memory failure. By constructing a spherical `BallTree` with the Haversine metric in radians, spatial querying operates in $\mathcal{O}(N \log M)$ time, computing all 1.5 million nearest neighbors in just 4.2 seconds on standard commodity hardware."

**Q4: Why maintain both a PostgreSQL Data Warehouse and Kafka Streaming?**  
- *Answer:* "They solve fundamentally different latency and granularity requirements. Kafka is an ephemeral event bus optimized for high-throughput, low-latency streaming (<15ms) to alert park rangers immediately. PostgreSQL is an OLAP dimensional store optimized for complex multi-year queries, historical carbon accounting, and regulatory compliance. Combining both gives us operational speed and analytical depth."
