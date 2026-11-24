#!/usr/bin/env python3
"""
generate_presentation.py — Generates a professional 16:9 PowerPoint presentation (.pptx)
for the Final Project Defense / Socialization.

Strictly adheres to the Evaluation Rubric:
  1. Project Evolution & Timeline (Deliveries 1 -> 2 -> Final & Feedback Corrections)
  2. Technical Decisions & Engineering Rationale (DWH Star Schema, GX Quality Gate, Kafka Event Design)
  3. Engineering Challenges & Solutions (Airflow 3 FastAPI Auth, Spatial BallTree, Big Data scale)
  4. Live Demo Strategy (Analytical Validation + Real-Time Streaming Inferences)
  5. 100% English Slides & Complete English Speaker Notes (10-15 minutes timing)
"""

import os
from pathlib import Path
import pptx
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE

PROJECT_ROOT = Path(__file__).parent.parent
OUTPUT_PPTX = PROJECT_ROOT / "presentation_deforestation_alert_etl.pptx"

# Color Palette (Professional Emerald & Slate Dark)
BG_DARK      = RGBColor(15, 23, 42)     # #0F172A
BG_CARD      = RGBColor(248, 250, 252) # #F8FAFC
TEXT_DARK    = RGBColor(30, 41, 59)    # #1E293B
TEXT_MUTED   = RGBColor(100, 116, 139) # #64748B
TEXT_LIGHT   = RGBColor(241, 245, 249) # #F1F5F9
PRIMARY_EMERALD = RGBColor(5, 150, 105) # #059669
FOREST_DARK  = RGBColor(6, 78, 59)     # #064E3B
ACCENT_TEAL  = RGBColor(14, 165, 233)  # #0EA5E9
ACCENT_AMBER = RGBColor(217, 119, 6)   # #D97706
ACCENT_RED   = RGBColor(220, 38, 38)   # #DC2626
BORDER_COLOR = RGBColor(203, 213, 225) # #CBD5E1


def create_header(slide, title_text, category_text):
    """Adds a standard modern header to a slide."""
    # Category tag
    cat_box = slide.shapes.add_textbox(Inches(0.8), Inches(0.4), Inches(10), Inches(0.35))
    tf_c = cat_box.text_frame
    tf_c.word_wrap = True
    tf_c.margin_left = tf_c.margin_right = tf_c.margin_top = tf_c.margin_bottom = 0
    p_c = tf_c.paragraphs[0]
    p_c.text = category_text.upper()
    p_c.font.size = Pt(11)
    p_c.font.bold = True
    p_c.font.color.rgb = PRIMARY_EMERALD
    p_c.font.name = "Calibri"

    # Main Title
    title_box = slide.shapes.add_textbox(Inches(0.8), Inches(0.72), Inches(11.7), Inches(0.65))
    tf_t = title_box.text_frame
    tf_t.word_wrap = True
    tf_t.margin_left = tf_t.margin_right = tf_t.margin_top = tf_t.margin_bottom = 0
    p_t = tf_t.paragraphs[0]
    p_t.text = title_text
    p_t.font.size = Pt(22)
    p_t.font.bold = True
    p_t.font.color.rgb = TEXT_DARK
    p_t.font.name = "Calibri"


def add_card(slide, left, top, width, height, bg_color=BG_CARD, border_color=BORDER_COLOR):
    """Draws a rounded card container for content grouping."""
    shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height)
    shape.fill.solid()
    shape.fill.fore_color.rgb = bg_color
    if border_color:
        shape.line.color.rgb = border_color
        shape.line.width = Pt(1.5)
    else:
        shape.line.fill.background()
    return shape


def add_bullet_list(shape_or_tf, items, font_size=13, space_after=8, text_color=TEXT_DARK):
    """Populates clean bullet points into a text frame."""
    tf = shape_or_tf if hasattr(shape_or_tf, "paragraphs") else shape_or_tf.text_frame
    for idx, item in enumerate(items):
        p = tf.add_paragraph() if idx > 0 or len(tf.paragraphs[0].text) > 0 else tf.paragraphs[0]
        p.text = item
        p.font.size = Pt(font_size)
        p.font.color.rgb = text_color
        p.font.name = "Calibri"
        p.space_after = Pt(space_after)
        p.level = 0


def build_presentation():
    prs = pptx.Presentation()
    # 16:9 Widescreen standard
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)
    blank_layout = prs.slide_layouts[6]

    # =========================================================================
    # SLIDE 1: Title Slide (Dark Aesthetic)
    # =========================================================================
    s1 = prs.slides.add_slide(blank_layout)
    bg1 = s1.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, prs.slide_width, prs.slide_height)
    bg1.fill.solid()
    bg1.fill.fore_color.rgb = BG_DARK
    bg1.line.fill.background()

    # Title box
    tbox = s1.shapes.add_textbox(Inches(1.0), Inches(1.8), Inches(11.333), Inches(3.5))
    tf = tbox.text_frame
    tf.word_wrap = True

    p0 = tf.paragraphs[0]
    p0.text = "ACADEMIC FINAL DEFENSE • ETL (G51)"
    p0.font.size = Pt(13)
    p0.font.bold = True
    p0.font.color.rgb = ACCENT_TEAL
    p0.font.name = "Calibri"
    p0.space_after = Pt(14)

    p1 = tf.add_paragraph()
    p1.text = "Deforestation Alert ETL & Real-Time ML System"
    p1.font.size = Pt(36)
    p1.font.bold = True
    p1.font.color.rgb = TEXT_LIGHT
    p1.font.name = "Calibri"
    p1.space_after = Pt(12)

    p2 = tf.add_paragraph()
    p2.text = "A Dual-Value Architecture: Analytical Dimensional Warehouse & Operational Streaming Inferences for Bolivia & Colombia"
    p2.font.size = Pt(18)
    p2.font.color.rgb = RGBColor(148, 163, 184)
    p2.font.name = "Calibri"
    p2.space_after = Pt(28)

    p3 = tf.add_paragraph()
    p3.text = "Master's Program in Data Engineering & Artificial Intelligence • Faculty of Engineering"
    p3.font.size = Pt(13)
    p3.font.color.rgb = PRIMARY_EMERALD
    p3.font.name = "Calibri"

    s1.notes_slide.notes_text_frame.text = (
        "[Time: 0:00 - 1:00 | Intro]\n"
        "Good morning, professor and colleagues. Today, our team presents the final delivery of our engineering project: "
        "'Deforestation Alert ETL & Real-Time ML Prediction System for Bolivia and Colombia'.\n"
        "Over the course of this academic term, we designed, deployed, and refined an end-to-end data architecture that tackles "
        "one of the most urgent environmental crises in South America: tropical deforestation. "
        "Crucially, our system proves how a single unified data foundation can generate dual business value: "
        "strategic analytical reporting through an Airflow-orchestrated dimensional warehouse, and tactical, sub-second operational decision-making "
        "through Apache Kafka and Machine Learning. Let us walk you through the evolution, architecture, and live demonstration."
    )

    # =========================================================================
    # SLIDE 2: Project Evolution & Timeline
    # =========================================================================
    s2 = prs.slides.add_slide(blank_layout)
    create_header(s2, "Project Evolution & Milestone Timeline", "Project Roadmap & Adaptation")

    # 3 Columns for 3 Deliveries
    col_w = Inches(3.64)
    gap = Inches(0.4)
    top_pos = Inches(1.55)
    h_pos = Inches(5.3)

    # Delivery 1
    add_card(s2, Inches(0.8), top_pos, col_w, h_pos)
    tb1 = s2.shapes.add_textbox(Inches(0.95), top_pos + Inches(0.15), col_w - Inches(0.3), h_pos - Inches(0.3))
    tf1 = tb1.text_frame
    tf1.word_wrap = True
    p = tf1.paragraphs[0]
    p.text = "Phase 1: Foundation\n(Delivery 1)"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = FOREST_DARK
    p.space_after = Pt(14)
    add_bullet_list(tf1, [
        "Initial Scope: Single source ingestion (Global Forest Watch raw satellite alerts).",
        "Local Exploratory Analysis: Baseline data inspection with Jupyter Notebooks.",
        "Initial Assumptions: Data volume was manageable locally without formal orchestrators or quality barriers.",
        "Outcome: Identified critical need for automated pipelines and multi-source context."
    ], font_size=12, space_after=10)

    # Delivery 2
    add_card(s2, Inches(0.8) + col_w + gap, top_pos, col_w, h_pos)
    tb2 = s2.shapes.add_textbox(Inches(0.95) + col_w + gap, top_pos + Inches(0.15), col_w - Inches(0.3), h_pos - Inches(0.3))
    tf2 = tb2.text_frame
    tf2.word_wrap = True
    p = tf2.paragraphs[0]
    p.text = "Phase 2: Modernization\n(Delivery 2 & Feedback)"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = ACCENT_AMBER
    p.space_after = Pt(14)
    add_bullet_list(tf2, [
        "Multi-Source Ingestion: Integrated World Bank (macroeconomics), FAOSTAT (crops/soy), and GeoNames (61k populated places).",
        "AWS Cloud Architecture: Deployed on AWS Lightsail with Amazon S3 Bronze Data Lake.",
        "Critical Corrections from Feedback:",
        "  • 10k sampling rejected: Upgraded to 100% full dataset validation (~1.5M rows) via Great Expectations.",
        "  • Atomic DAG redesign: Broken down monolithic tasks into atomic Medallion steps.",
        "  • Spatial join refactored: Replaced Cartesian merges with spherical BallTree Haversine."
    ], font_size=11, space_after=8)

    # Final Delivery
    add_card(s2, Inches(0.8) + (col_w + gap) * 2, top_pos, col_w, h_pos)
    tb3 = s2.shapes.add_textbox(Inches(0.95) + (col_w + gap) * 2, top_pos + Inches(0.15), col_w - Inches(0.3), h_pos - Inches(0.3))
    tf3 = tb3.text_frame
    tf3.word_wrap = True
    p = tf3.paragraphs[0]
    p.text = "Phase 3: Final Delivery\n(Dual Value & Streaming)"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = PRIMARY_EMERALD
    p.space_after = Pt(14)
    add_bullet_list(tf3, [
        "ML Risk Classifier: Trained and serialized Random Forest pipeline (92.8% Accuracy, 0.9789 ROC-AUC).",
        "Apache Kafka Streaming: Implemented producer & consumer simulating real-time alert traffic.",
        "Zero-Data Leakage: Target variable strictly stripped from streaming messages.",
        "Dual Consumption Paths:",
        "  • Static Analytical Report in Python (IPCC Tier 1 carbon & drivers).",
        "  • Live Streamlit 3D Map Radar updating in real-time."
    ], font_size=11, space_after=8)

    s2.notes_slide.notes_text_frame.text = (
        "[Time: 1:00 - 2:30 | Evolution & Timeline]\n"
        "To understand our engineering approach, we must review how this system evolved. "
        "In Delivery 1, we started with a single CSV file from Global Forest Watch. We quickly realized that satellite coordinates alone "
        "could not answer the 'why' behind deforestation.\n"
        "In Delivery 2, we incorporated three additional sources: World Bank macro indicators, FAO agricultural yields, and GeoNames populated places. "
        "Crucially, we received strict technical feedback: our initial quality check only sampled 10,000 rows, our ETL task was monolithic, "
        "and our spatial joins risked data explosion. We listened and addressed every single observation: "
        "we overhauled Great Expectations to validate 100% of the 1.5 million records, restructured the Airflow DAG into atomic Medallion tasks, "
        "and replaced non-spatial merges with a spherical BallTree Haversine distance index.\n"
        "Finally, for this Final Delivery, we transformed the batch warehouse into a dual-stream architecture with Apache Kafka and Machine Learning."
    )

    # =========================================================================
    # SLIDE 3: High-Level Architecture (Dual Value)
    # =========================================================================
    s3 = prs.slides.add_slide(blank_layout)
    create_header(s3, "End-to-End System Architecture", "Dual-Value Engineering Design")

    # Left: Image of architecture
    arch_img = PROJECT_ROOT / "docs" / "img" / "architecture.png"
    if arch_img.exists():
        s3.shapes.add_picture(str(arch_img), Inches(0.8), Inches(1.5), Inches(6.8), Inches(5.3))

    # Right: Description Card
    card_r = add_card(s3, Inches(7.8), Inches(1.5), Inches(4.7), Inches(5.3))
    tbr = s3.shapes.add_textbox(Inches(8.0), Inches(1.65), Inches(4.3), Inches(5.0))
    tfr = tbr.text_frame
    tfr.word_wrap = True
    pr = tfr.paragraphs[0]
    pr.text = "Architectural Pillars"
    pr.font.size = Pt(17)
    pr.font.bold = True
    pr.font.color.rgb = FOREST_DARK
    pr.space_after = Pt(12)

    add_bullet_list(tfr, [
        "Bronze Data Lake (S3): Raw data is uploaded immutably prior to cleaning, ensuring complete lineage and auditability.",
        "Quality Gate Barrier: Great Expectations halts the pipeline if nulls, out-of-range coordinates, or empty datasets are detected.",
        "Silver Transformation Layer: Geodesic Haversine BallTree indexes distance to 61k populated settlements in O(N log M) time.",
        "Path A — Analytical Data Warehouse: PostgreSQL Star Schema feeding historical IPCC Tier 1 carbon accounting.",
        "Path B — Operational Kafka Stream: Decoupled producer/consumer executing ML inferences with sub-15ms latency."
    ], font_size=12, space_after=10)

    s3.notes_slide.notes_text_frame.text = (
        "[Time: 2:30 - 4:00 | Architecture]\n"
        "Here on Slide 3 is our high-level architecture diagram. As you can see, data follows a disciplined Medallion pattern: "
        "Raw satellite files, World Bank REST endpoints, FAO CSV dumps, and GeoNames are ingested in parallel and stored immutably in Amazon S3 Bronze.\n"
        "Next comes our mandatory Quality Gate. If any file is corrupt or empty, execution halts immediately. "
        "In the Silver layer, data is cleaned and enriched with spherical Haversine math. "
        "From this stage, the data splits into two distinct value chains: "
        "The Analytical path writes to our PostgreSQL Star Schema for longitudinal policy reports; "
        "while the Operational path streams live alert vectors through Apache Kafka to our serialized Machine Learning model for instant ranger dispatch."
    )

    # =========================================================================
    # SLIDE 4: Data Quality & Airflow DAG
    # =========================================================================
    s4 = prs.slides.add_slide(blank_layout)
    create_header(s4, "Data Quality Gate & Atomic Airflow Orchestration", "Robustness & Reproducibility")

    # Left: DAG Image
    dag_img = PROJECT_ROOT / "docs" / "img" / "deforestation_etl_dag-graph.png"
    if dag_img.exists():
        s4.shapes.add_picture(str(dag_img), Inches(0.8), Inches(1.5), Inches(6.5), Inches(5.3))

    # Right: Great Expectations card
    card_gx = add_card(s4, Inches(7.5), Inches(1.5), Inches(5.0), Inches(5.3))
    tbg = s4.shapes.add_textbox(Inches(7.7), Inches(1.65), Inches(4.6), Inches(5.0))
    tfg = tbg.text_frame
    tfg.word_wrap = True
    pg = tfg.paragraphs[0]
    pg.text = "Quality Assurance & Orchestration"
    pg.font.size = Pt(17)
    pg.font.bold = True
    pg.font.color.rgb = FOREST_DARK
    pg.space_after = Pt(12)

    add_bullet_list(tfg, [
        "100% Full-Dataset Audit: Replaced sampling. Validates all 1,523,990 rows across 6 expectation suites in under 3 seconds.",
        "Zero-Tolerance for Empty Datasets: ExpectTableRowCountToBeBetween(min_value=1) prevents ghost runs from polluting tables.",
        "Strict Geospatial & Value Bounds: Evaluates lat [-90,90], lon [-180,180], tree cover density [0,100%], and confidence categories.",
        "Atomic DAG Design: 6 decoupled stages (Ingest -> S3 -> GX Quality Gate -> Silver Enrich -> DWH Load -> Reporting).",
        "Airflow 3 Native Security: Configured SimpleAuthManager with role-based JWT authentication (admin:ADMIN)."
    ], font_size=12, space_after=10)

    s4.notes_slide.notes_text_frame.text = (
        "[Time: 4:00 - 5:30 | Quality & DAG]\n"
        "Turning to Slide 4, we highlight how we transformed our orchestration and data quality. "
        "A critical feedback point was that accepting empty datasets or sub-sampling 10k rows is unacceptable in modern enterprise architectures. "
        "We completely re-architected validate_quality.py with Great Expectations: today, it audits 100% of our 1.5 million rows across 6 suites.\n"
        "If an upstream API returns an empty CSV or if coordinates fall outside South America, the task immediately fails with exit code 1, "
        "preventing bad data from ever reaching the Data Warehouse.\n"
        "Furthermore, our Airflow DAG was decoupled into discrete, atomic operators scheduled nightly at 22:00 UTC."
    )

    # =========================================================================
    # SLIDE 5: Dimensional Data Warehouse (Star Schema)
    # =========================================================================
    s5 = prs.slides.add_slide(blank_layout)
    create_header(s5, "Dimensional Data Warehouse: Star Schema Design", "Analytics Data Modeling")

    # Card 1: Fact Table
    c1 = add_card(s5, Inches(0.8), Inches(1.5), Inches(5.7), Inches(5.3))
    tb_c1 = s5.shapes.add_textbox(Inches(1.0), Inches(1.65), Inches(5.3), Inches(5.0))
    tfc1 = tb_c1.text_frame
    tfc1.word_wrap = True
    p = tfc1.paragraphs[0]
    p.text = "Central Fact Table: fact_alerts"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = ACCENT_AMBER
    p.space_after = Pt(10)
    add_bullet_list(tfc1, [
        "Grain: Single satellite pixel alert event (GLAD/RADD) per geographic coordinate and timestamp.",
        "Surrogate Foreign Keys:",
        "  • date_id -> dim_date (Year, Month, Week, Day)",
        "  • location_id -> dim_location (ISO, Country, Dept)",
        "  • driver_id -> dim_driver (Direct loss cause)",
        "  • land_cover_id -> dim_land_cover",
        "  • confidence_id -> dim_confidence",
        "  • econ_id -> dim_economic_context (WB + FAO)",
        "Additive & Spatial Metrics: tree_cover_density_pct, is_primary_forest, dist_nearest_place_km."
    ], font_size=11, space_after=6)

    # Card 2: Dimension Decoupling & Indexes
    c2 = add_card(s5, Inches(6.8), Inches(1.5), Inches(5.7), Inches(5.3))
    tb_c2 = s5.shapes.add_textbox(Inches(7.0), Inches(1.65), Inches(5.3), Inches(5.0))
    tfc2 = tb_c2.text_frame
    tfc2.word_wrap = True
    p = tfc2.paragraphs[0]
    p.text = "Dimension Modeling & Performance"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = FOREST_DARK
    p.space_after = Pt(10)
    add_bullet_list(tfc2, [
        "Preventing Macroeconomic Duplication: Rather than repeating annual GDP or soy yield across 1.5M alerts, dim_economic_context normalizes data by (country_code, year).",
        "dim_places (GeoNames): 61,749 populated settlements preserved independently with spherical coordinate metadata.",
        "Database Indexing Strategy:",
        "  • B-Tree indexes on all foreign keys (idx_fact_date, idx_fact_location, idx_fact_driver, idx_fact_econ).",
        "  • Fast analytical slice-and-dice for longitudinal queries and IPCC Tier 1 carbon aggregation."
    ], font_size=11, space_after=8)

    s5.notes_slide.notes_text_frame.text = (
        "[Time: 5:30 - 7:00 | Star Schema]\n"
        "On Slide 5, we present our relational dimensional design in PostgreSQL. "
        "A common pitfall in spatial ETL is creating unspaced joins that duplicate macro indicators millions of times. "
        "We designed a clean Star Schema where fact_alerts maintains the finest grain: individual satellite alert detections.\n"
        "We decoupled macroeconomic indicators into dim_economic_context, populated by country and year. "
        "Each alert references this dimension through a surrogate foreign key, ensuring zero redundancy. "
        "Furthermore, by building B-Tree indexes across all foreign keys, our static analytical queries execute in milliseconds over 1.5 million rows."
    )

    # =========================================================================
    # SLIDE 6: Machine Learning Pipeline & Zero-Leakage
    # =========================================================================
    s6 = prs.slides.add_slide(blank_layout)
    create_header(s6, "Machine Learning: Predicting Primary Forest Risk", "Predictive Intelligence")

    # Metrics Card Left
    c_m1 = add_card(s6, Inches(0.8), Inches(1.5), Inches(5.7), Inches(5.3))
    tbm1 = s6.shapes.add_textbox(Inches(1.0), Inches(1.65), Inches(5.3), Inches(5.0))
    tfm1 = tbm1.text_frame
    tfm1.word_wrap = True
    p = tfm1.paragraphs[0]
    p.text = "Model Formulation & Performance"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = FOREST_DARK
    p.space_after = Pt(10)
    add_bullet_list(tfm1, [
        "Prediction Target: is_primary_forest (Binary: 1 = Intact Virgin Forest / Ecological Disaster; 0 = Secondary Regrowth).",
        "Why It Matters: Virgin tropical forests possess irreplaceable biodiversity and massive carbon reservoirs; secondary scrubs do not.",
        "Algorithm: Stratified Random Forest Classifier Pipeline.",
        "Validation Results (40,000-sample Test Set):",
        "  • Accuracy: 92.77%",
        "  • F1-Score (Weighted): 92.71%",
        "  • ROC-AUC Score: 0.9789",
        "Confusion Matrix: 18,728 True Negatives, 18,380 True Positives (balanced, low false alarm rate)."
    ], font_size=11, space_after=6)

    # Preprocessing Card Right
    c_m2 = add_card(s6, Inches(6.8), Inches(1.5), Inches(5.7), Inches(5.3))
    tbm2 = s6.shapes.add_textbox(Inches(7.0), Inches(1.65), Inches(5.3), Inches(5.0))
    tfm2 = tbm2.text_frame
    tfm2.word_wrap = True
    p = tfm2.paragraphs[0]
    p.text = "Anti-Leakage & Serialization"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = ACCENT_TEAL
    p.space_after = Pt(10)
    add_bullet_list(tfm2, [
        "Strict Zero-Data Leakage Design: Target variable is entirely excluded from the Kafka streaming message. The model only receives contextual and geographic signals.",
        "Input Features: latitude, longitude, tree_cover_density_pct, dist_nearest_place_km, month, week, driver_id, country_code.",
        "Pipeline Architecture: Integrated SimpleImputer + Robust Scaler + OneHotEncoder into a single compiled object.",
        "Serialization: Saved as models/deforestation_forest_risk_pipeline.joblib. The Kafka consumer loads this artifact once and performs sub-15ms predictions without retraining."
    ], font_size=11, space_after=7)

    s6.notes_slide.notes_text_frame.text = (
        "[Time: 7:00 - 8:30 | Machine Learning]\n"
        "Slide 6 details our predictive modeling workflow. We defined a high-impact business problem: "
        "predicting whether an active deforestation alert threatens intact primary forest. "
        "Secondary regrowth loss is regrettable, but primary forest destruction is irreversible.\n"
        "To avoid data leakage, the target variable is never sent over Kafka. The model predicts solely based on coordinates, "
        "forest canopy density, distance to settlements from GeoNames, calendar temporality, and driver category.\n"
        "Our serialized Random Forest pipeline achieved an outstanding 92.8% accuracy and 0.9789 ROC-AUC on a rigorous test split. "
        "The model is fully bundled into a single joblib file, allowing our Kafka consumer to predict without overhead."
    )

    # =========================================================================
    # SLIDE 7: Kafka Event Streaming Architecture
    # =========================================================================
    s7 = prs.slides.add_slide(blank_layout)
    create_header(s7, "Event Streaming Architecture with Apache Kafka", "Real-Time Pipeline")

    c_k1 = add_card(s7, Inches(0.8), Inches(1.5), Inches(5.7), Inches(5.3))
    tb_k1 = s7.shapes.add_textbox(Inches(1.0), Inches(1.65), Inches(5.3), Inches(5.0))
    tfk1 = tb_k1.text_frame
    tfk1.word_wrap = True
    p = tfk1.paragraphs[0]
    p.text = "Kafka Producer & Topic Design"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = FOREST_DARK
    p.space_after = Pt(10)
    add_bullet_list(tfk1, [
        "Topic Name: deforestation-alerts-stream.",
        "Message Schema (JSON):",
        "  • event_id (UUID / Unique string)",
        "  • timestamp (ISO-8601 UTC)",
        "  • latitude, longitude (Spatial coordinates)",
        "  • tree_cover_density_pct (Canopy density)",
        "  • dist_nearest_place_km (Proximity to human centers)",
        "  • country_code, month, week, driver_id",
        "Producer Resilience: Streams from fact records with controlled delays; includes persistent disk fallback if external broker is offline."
    ], font_size=11, space_after=6)

    c_k2 = add_card(s7, Inches(6.8), Inches(1.5), Inches(5.7), Inches(5.3))
    tb_k2 = s7.shapes.add_textbox(Inches(7.0), Inches(1.65), Inches(5.3), Inches(5.0))
    tfk2 = tb_k2.text_frame
    tfk2.word_wrap = True
    p = tfk2.paragraphs[0]
    p.text = "Kafka Consumer & Live Inference Engine"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = ACCENT_AMBER
    p.space_after = Pt(10)
    add_bullet_list(tfk2, [
        "Continuous Consumer Loop: Listens for incoming records on deforestation-alerts-stream topic.",
        "Single Model Ingestion: Loads serialized pipeline once into memory, preventing execution lag.",
        "Dual Output Inference: Runs predict() for discrete class and predict_proba() for calibrated confidence.",
        "Alert Prioritization: Tags predictions as CRITICAL (Primary Forest) or MODERATE (Secondary Forest).",
        "Direct Streaming to UI: Appends enriched predictions to realtime_predictions.jsonl to drive the Streamlit radar."
    ], font_size=11, space_after=8)

    s7.notes_slide.notes_text_frame.text = (
        "[Time: 8:30 - 9:45 | Kafka Streaming]\n"
        "On Slide 7, we examine our event streaming layer. In real-world disaster management, waiting for nightly batch loads is unacceptable. "
        "Our Python Kafka Producer simulates satellite feed ingestion, packaging alert vectors into lightweight JSON payloads.\n"
        "On the other end, our Kafka Consumer operates 24/7. It ingests each message, feeds it into our pre-loaded scikit-learn pipeline, "
        "and computes class probabilities in under 15 milliseconds. "
        "This transforms passive historical data into an active operational alert stream."
    )

    # =========================================================================
    # SLIDE 8: Dual-Value Delivery (Static vs Real-Time)
    # =========================================================================
    s8 = prs.slides.add_slide(blank_layout)
    create_header(s8, "Dual-Value Generation: Analytical vs. Operational", "Business Impact")

    # Table comparing the two paths
    col_w8 = Inches(5.7)
    c_d1 = add_card(s8, Inches(0.8), Inches(1.5), col_w8, Inches(5.3))
    tb_d1 = s8.shapes.add_textbox(Inches(1.0), Inches(1.65), col_w8 - Inches(0.4), Inches(5.0))
    tfd1 = tb_d1.text_frame
    tfd1.word_wrap = True
    p = tfd1.paragraphs[0]
    p.text = "📊 Path 1: Strategic Analytical Reporting"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = FOREST_DARK
    p.space_after = Pt(10)
    add_bullet_list(tfd1, [
        "Audience: Environmental Ministers, Economists, Climate Researchers.",
        "Engine: PostgreSQL DWH + Python (scripts/generate_static_report.py).",
        "Cadence: Nightly batch point-in-time snapshot.",
        "Core Business Questions Answered:",
        "  • What is the longitudinal deforestation trend in Bolivia vs Colombia (2022-2026)?",
        "  • How do soy harvest expansions correlate with tree canopy loss?",
        "  • What are the estimated Tier 1 IPCC carbon emissions (CO2e)?",
        "Output: Consolidated Markdown analytical brief with high-resolution statistical charts."
    ], font_size=11, space_after=7)

    c_d2 = add_card(s8, Inches(6.8), Inches(1.5), col_w8, Inches(5.3))
    tb_d2 = s8.shapes.add_textbox(Inches(7.0), Inches(1.65), col_w8 - Inches(0.4), Inches(5.0))
    tfd2 = tb_d2.text_frame
    tfd2.word_wrap = True
    p = tfd2.paragraphs[0]
    p.text = "⚡ Path 2: Operational Real-Time Dispatch"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = ACCENT_RED
    p.space_after = Pt(10)
    add_bullet_list(tfd2, [
        "Audience: Park Rangers, Forestry Brigades, Emergency Field Responders.",
        "Engine: Apache Kafka + ML Inference Pipeline + Streamlit / Pydeck.",
        "Cadence: Real-time event-driven streaming (< 15 ms latency).",
        "Core Operational Decisions Answered:",
        "  • Which deforestation front requires immediate deployment today?",
        "  • Is this alert inside intact primary forest or an authorized farm clearing?",
        "  • What is the distance to the nearest settlement for tactical logistics?",
        "Output: Interactive 3D geospatial radar map with live incident feed."
    ], font_size=11, space_after=7)

    s8.notes_slide.notes_text_frame.text = (
        "[Time: 9:45 - 11:00 | Dual Value]\n"
        "Slide 8 is the core value proposition of our project: Dual-Value Articulation. "
        "A common flaw in academic projects is adding Kafka or ML just to check a box. In our solution, both consumption paths solve "
        "radically different business problems.\n"
        "On the left, Path 1 is analytical and retrospective: it uses PostgreSQL to compute carbon balances and evaluate macro trends for policy makers. "
        "On the right, Path 2 is tactical and real-time: it uses Kafka and Machine Learning to dispatch park rangers to critical hotspots within seconds. "
        "One unified data architecture successfully serves both personas."
    )

    # =========================================================================
    # SLIDE 9: Technical Challenges & Key Learnings
    # =========================================================================
    s9 = prs.slides.add_slide(blank_layout)
    create_header(s9, "Technical Challenges & Engineering Breakthroughs", "Problem Solving")

    col_w9 = Inches(3.64)
    gap9 = Inches(0.4)

    # Challenge 1: Airflow 3 Auth
    add_card(s9, Inches(0.8), top_pos, col_w9, h_pos)
    tb_c1 = s9.shapes.add_textbox(Inches(0.95), top_pos + Inches(0.15), col_w9 - Inches(0.3), h_pos - Inches(0.3))
    tfc1 = tb_c1.text_frame
    tfc1.word_wrap = True
    p = tfc1.paragraphs[0]
    p.text = "Airflow 3 FastAPI Auth"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = ACCENT_RED
    p.space_after = Pt(12)
    add_bullet_list(tfc1, [
        "Point of Failure: Airflow 3 deprecated 'airflow users create' CLI; SimpleAuthManager failed JWT generation due to lowercase roles.",
        "Engineering Fix: Discovered users must be defined as 'admin:ADMIN' (uppercase role), set simple_auth_manager_all_admins=False, and provisioned a verified JSON passwords file.",
        "Result: Rock-solid, reproducible login with admin:admin locally and on Lightsail."
    ], font_size=11, space_after=9)

    # Challenge 2: Spatial Optimization
    add_card(s9, Inches(0.8) + col_w9 + gap9, top_pos, col_w9, h_pos)
    tb_c2 = s9.shapes.add_textbox(Inches(0.95) + col_w9 + gap9, top_pos + Inches(0.15), col_w9 - Inches(0.3), h_pos - Inches(0.3))
    tfc2 = tb_c2.text_frame
    tfc2.word_wrap = True
    p = tfc2.paragraphs[0]
    p.text = "Geospatial Big Data Scale"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = ACCENT_AMBER
    p.space_after = Pt(12)
    add_bullet_list(tfc2, [
        "Point of Failure: Naive Cartesian joins between 1.5M alerts and 61k GeoNames localities threatened out-of-memory crashes (91 billion pairs).",
        "Engineering Fix: Implemented scikit-learn's BallTree with spherical Haversine metric in radian coordinates.",
        "Result: Distance to nearest town computed in O(N log M) time in just 4.2 seconds with zero memory overflow."
    ], font_size=11, space_after=9)

    # Challenge 3: Schema Drift & Quality Gate
    add_card(s9, Inches(0.8) + (col_w9 + gap9) * 2, top_pos, col_w9, h_pos)
    tb_c3 = s9.shapes.add_textbox(Inches(0.95) + (col_w9 + gap9) * 2, top_pos + Inches(0.15), col_w9 - Inches(0.3), h_pos - Inches(0.3))
    tfc3 = tb_c3.text_frame
    tfc3.word_wrap = True
    p = tfc3.paragraphs[0]
    p.text = "Schema Drift & Zero-Leakage"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = FOREST_DARK
    p.space_after = Pt(12)
    add_bullet_list(tfc3, [
        "Point of Failure: External column renaming and target variable leakage during streaming simulation.",
        "Engineering Fix: Standardized schema mapping in Silver layer (is_high_confidence) and built explicit payload constructors stripping target is_primary_forest.",
        "Result: Bulletproof DWH ingestion and authentic ML inference."
    ], font_size=11, space_after=9)

    s9.notes_slide.notes_text_frame.text = (
        "[Time: 11:00 - 12:15 | Challenges]\n"
        "Engineering is about solving tough problems under constraints. Slide 9 outlines our three biggest breakthroughs. "
        "First, Airflow 3 completely restructured user authentication around FastAPI and SimpleAuthManager, causing standard CLI scripts to fail. "
        "We diagnosed that roles must be strictly uppercase (admin:ADMIN) with custom password JSON files to successfully generate JWT tokens.\n"
        "Second, cross-referencing 1.5 million alerts with 61,000 GeoNames points would create 91 billion combinations if done naively. "
        "We implemented a spherical BallTree with Haversine metrics, completing the query in 4 seconds.\n"
        "Third, we eliminated schema drift and guaranteed zero data leakage across our streaming buses."
    )

    # =========================================================================
    # SLIDE 10: Live Demonstration Walkthrough
    # =========================================================================
    s10 = prs.slides.add_slide(blank_layout)
    create_header(s10, "Live Demonstration Roadmap", "System Verification")

    c_demo1 = add_card(s10, Inches(0.8), Inches(1.5), Inches(5.7), Inches(5.3))
    tb_dm1 = s10.shapes.add_textbox(Inches(1.0), Inches(1.65), Inches(5.3), Inches(5.0))
    tfdm1 = tb_dm1.text_frame
    tfdm1.word_wrap = True
    p = tfdm1.paragraphs[0]
    p.text = "Demo Part 1: Analytical Pipeline & DWH"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = FOREST_DARK
    p.space_after = Pt(10)
    add_bullet_list(tfdm1, [
        "1. Airflow Orchestration & Auth: Login to web interface (:9179) with admin:admin; inspect green DAG execution.",
        "2. Great Expectations Quality Gate: Show 100% data pass across 1.5M rows.",
        "3. PostgreSQL Star Schema: Verify dimensional tables (dim_date, dim_location, dim_driver, fact_alerts).",
        "4. Static Analytical Report: Execute python scripts/generate_static_report.py to show output charts and IPCC Tier 1 carbon metrics."
    ], font_size=12, space_after=10)

    c_demo2 = add_card(s10, Inches(6.8), Inches(1.5), Inches(5.7), Inches(5.3))
    tb_dm2 = s10.shapes.add_textbox(Inches(7.0), Inches(1.65), Inches(5.3), Inches(5.0))
    tfdm2 = tb_dm2.text_frame
    tfdm2.word_wrap = True
    p = tfdm2.paragraphs[0]
    p.text = "Demo Part 2: Kafka Streaming & Streamlit Radar"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = ACCENT_AMBER
    p.space_after = Pt(10)
    add_bullet_list(tfdm2, [
        "1. Launch Real-Time Dashboard: Show live 3D Pydeck radar map running on Streamlit (:8501).",
        "2. Trigger Kafka Producer: Inject batch of live satellite alert events via terminal or UI button.",
        "3. Live Consumer Inferences: Observe sub-15ms ML predictions assigning critical vs moderate badges.",
        "4. Dynamic UI Update: Show real-time map lighting up with bright red hotspots for intact primary forest threats."
    ], font_size=12, space_after=10)

    s10.notes_slide.notes_text_frame.text = (
        "[Time: 12:15 - 13:30 | Live Demo Guide]\n"
        "Now, we invite you to witness our live demonstration. We will prove both fronts operating simultaneously. "
        "First, we will verify our validated Data Warehouse and execute our static reporting engine. "
        "Second, we will launch our Streamlit dashboard, fire our Kafka streaming producer, and observe live ML inferences "
        "populating the 3D map in real-time."
    )

    # =========================================================================
    # SLIDE 11: Real-World Impact & SDG Alignment
    # =========================================================================
    s11 = prs.slides.add_slide(blank_layout)
    create_header(s11, "Global Impact: UN Sustainable Development Goals", "Sustainability & Policy")

    col_w11 = Inches(5.7)
    c_s1 = add_card(s11, Inches(0.8), Inches(1.5), col_w11, Inches(5.3))
    tb_s1 = s11.shapes.add_textbox(Inches(1.0), Inches(1.65), col_w11 - Inches(0.4), Inches(5.0))
    tfs1 = tb_s1.text_frame
    tfs1.word_wrap = True
    p = tfs1.paragraphs[0]
    p.text = "🌍 SDG 13: Climate Action"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = FOREST_DARK
    p.space_after = Pt(10)
    add_bullet_list(tfs1, [
        "IPCC Tier 1 Carbon Modeling: Deforestation is translated directly into metric tons of CO2e released to the atmosphere.",
        "Agricultural Frontier Accountability: Cross-referencing FAO soy yield with loss drivers identifies commercial supply chain emissions.",
        "Compliance with EU Deforestation Regulation (EUDR): Enables auditable verification for export commodities."
    ], font_size=12, space_after=12)

    c_s2 = add_card(s11, Inches(6.8), Inches(1.5), col_w11, Inches(5.3))
    tb_s2 = s11.shapes.add_textbox(Inches(7.0), Inches(1.65), col_w11 - Inches(0.4), Inches(5.0))
    tfs2 = tb_s2.text_frame
    tfs2.word_wrap = True
    p = tfs2.paragraphs[0]
    p.text = "🐆 SDG 15: Life on Land"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = PRIMARY_EMERALD
    p.space_after = Pt(10)
    add_bullet_list(tfs2, [
        "Intact Forest Prioritization: Focusing emergency response on primary forest safeguards indigenous territories and endemic species.",
        "Human Pressure Quantification: Calculating distance to 61k populated settlements maps expanding penetration roads.",
        "Protected Areas Surveillance: Integrates national park buffers into threat classification."
    ], font_size=12, space_after=12)

    s11.notes_slide.notes_text_frame.text = (
        "[Time: 13:30 - 14:15 | Impact & SDGs]\n"
        "Before concluding, we want to highlight the environmental significance of this work. "
        "Our engineering design directly advances United Nations Sustainable Development Goals 13 and 15. "
        "By translating satellite alerts into IPCC Tier 1 carbon metrics and tracking soy expansion, we provide the empirical basis "
        "for international compliance like the European Union Deforestation Regulation. "
        "Simultaneously, by prioritizing primary forests in real time, we help conserve the lungs of our planet."
    )

    # =========================================================================
    # SLIDE 12: Conclusion & Q&A (Dark Aesthetic)
    # =========================================================================
    s12 = prs.slides.add_slide(blank_layout)
    bg12 = s12.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, prs.slide_width, prs.slide_height)
    bg12.fill.solid()
    bg12.fill.fore_color.rgb = BG_DARK
    bg12.line.fill.background()

    tb12 = s12.shapes.add_textbox(Inches(1.0), Inches(2.0), Inches(11.333), Inches(3.5))
    tf12 = tb12.text_frame
    tf12.word_wrap = True

    p = tf12.paragraphs[0]
    p.text = "SUMMARY OF ACHIEVEMENTS"
    p.font.size = Pt(13)
    p.font.bold = True
    p.font.color.rgb = PRIMARY_EMERALD
    p.space_after = Pt(12)

    p1 = tf12.add_paragraph()
    p1.text = "Thank You! Questions & Discussion"
    p1.font.size = Pt(36)
    p1.font.bold = True
    p1.font.color.rgb = TEXT_LIGHT
    p1.space_after = Pt(16)

    p2 = tf12.add_paragraph()
    p2.text = "✔ Multi-Source Medallion ETL (Airflow 3 + S3 Bronze + PostgreSQL Star Schema)\n" \
              "✔ 100% Full-Dataset Quality Assurance with Great Expectations (~1.5M rows)\n" \
              "✔ Real-Time Kafka Streaming & Serialized ML Random Forest (92.8% Acc, 0.9789 ROC-AUC)\n" \
              "✔ Dual Consumption: Strategic Static Report & Interactive Streamlit 3D Map"
    p2.font.size = Pt(15)
    p2.font.color.rgb = RGBColor(148, 163, 184)
    p2.space_after = Pt(24)

    p3 = tf12.add_paragraph()
    p3.text = "GitHub Repository: github.com/JhoanHurtado/deforestation-alert-etl"
    p3.font.size = Pt(14)
    p3.font.color.rgb = ACCENT_TEAL

    s12.notes_slide.notes_text_frame.text = (
        "[Time: 14:15 - 15:00 | Conclusion & Q&A]\n"
        "In summary, our final delivery meets and exceeds every rubric requirement. "
        "We evolved from a simple script to a modern, cloud-ready, dual-value data platform. "
        "Thank you very much for your time and guidance throughout this course. "
        "We are now open and eager to answer any questions."
    )

    prs.save(OUTPUT_PPTX)
    print(f"✅ PowerPoint presentation successfully generated at: {OUTPUT_PPTX}")


if __name__ == "__main__":
    build_presentation()
