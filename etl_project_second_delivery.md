uao  
Universidad  
Autónoma de  
Occidente  

FACULTY OF ENGINEERING AND BASIC SCIENCES  
ACADEMIC PROGRAM: DATA ENGINEERING AND ARTIFICIAL INTELLIGENCE  
COURSE: ETL (G51)  

# ETL-Project: Second Delivery

### Deadline
The project must be submitted through the platform by the date discussed in class.

### Teammate
You must collaborate with your colleagues, which means you must form a group of at least 2 people and no more than 5.

### Objectives
This second delivery focuses on automating the data ingestion and transformation process using Airflow, collecting data from a public API, ensuring data quality with Great Expectations, and building visualizations on top of the validated data.

At a minimum, you must use Airflow as the orchestrator and Great Expectations (or similar tools). You are welcome to use additional tools if you prefer.

By the end of this stage, you should have a reproducible and automated data pipeline capable of:
* Extracting data periodically from an API, database or scraping.
* Transforming and storing it in a relational database.
* Running automated data quality checks.
* Generating visualizations from validated datasets.

---

### What is Expected

#### 1. Requirements gathering
The purpose of this stage is to clearly define the objectives, data needs, and expected outcomes of the data analysis task before building the pipeline. This step should answer "Why are we building this pipeline?" and "What insights should the data deliver?"

Consider the new data in the refined objectives.

#### 2. Data ingestion
* Use the data of the first delivery.
* Use another dataset that can be used for your problem and store it in a database.
* Select a public API that provides structured and meaningful data according to your problem.
* Implement three different Airflow tasks to automatically extract data from the three sources at scheduled intervals (e.g., daily).

#### 3. Exploratory Data Analysis (EDA)
Perform EDA on all the data to understand their structure and quality:
* Check data types, missing values, duplicates, and outliers.
* Identify key variables (e.g., artist, song, year, award type, streams).
* Generate basic statistics and plots (e.g., distributions, top categories).
* Document insights from the EDA that will guide your transformations.

You can do the EDA in a Jupyter Notebook.

#### 4. Data transformation
* Clean and normalize the ingested data (e.g., clean missing values, format columns, filter relevant fields).
* Perform transformations to align the data (e.g., date formats, categorical values).
* Merge all the data into a single, enriched dataset.

#### 5. Data quality validation with Great Expectations
* Use Great Expectations to evaluate the quality of your data.
* Define and implement validation suites according to your problem.
* Integrate the validation step into the Airflow DAG.
* Log validation results and only allow loading into the final schema if checks pass.

#### 6. Data Load
* Design and document the data model (e.g., star or snowflake schema).
* Load the transformed data into your storage.

#### 7. Visualization
Create visualizations and dashboards from the validated data stored in the Data Warehouse using tools such as:

The visualizations should provide business-relevant insights based on the project objectives refined.

#### 8. Deliverables
* Include a brief technical document that explains:
  * Refined objectives
  * Summary of the EDA process and the visualizations produced
  * API details and why it was chosen,
  * Updated architecture diagram (including Airflow and validation),
  * Description of the ETL pipeline steps,
  * Airflow DAG design.
  * Assumptions and decisions made during transformations.
  * Great Expectations validation suite summary,
  * Examples of visualizations and insights.
* Upload your code and diagrams to GitHub as a portfolio project.
  * README.md explaining your project (The ETL pipeline steps, Airflow DAG design, Assumptions and decisions made during transformations), setup instructions, and key decisions.
  * Visualizations
  * .gitignore file.

---

### Technology Stack

| Layer | Tool/Technology | Purpose |
| :--- | :--- | :--- |
| Orchestration | Apache Airflow | Automate and schedule ETL workflows |
| Data Source | Public API | Provide fresh external data |
| Data Storage | PostgreSQL/MySQL | Persist clean and structured data |
| Quality Checks | Great Expectations | Validate data before loading |
| Transformation | Python (Pandas) | Data cleaning and formatting |
| Visualization | Power BI, Native, etc. | Insights and dashboards |
| Documentation & Versioning | GitHub + Markdown | Project tracking and version control |

---

### Diagram
Example diagram is provided in figure 1 (overall project flow).

```
[API (www)] ---> [Extract (API)] ---> [Transform] -----\
                                                        \
[CSV dataset] -> [read_csv] --------> [Transform] -----> [Merge] ---> [Quality Check] ---> [Load] ---> [Data Warehouse (SQL)]
                                                        /                                                |
[SQL DB] ------> [read_db] ---------> [Transform] -----/                                                 v
                                                                                                  [Visualizations]
```

**Figure 1. Block Diagram**

---

### Evaluation

| Item | Description | Weight |
| :--- | :--- | :--- |
| **Refined requirements & EDA** | Clearly defines objectives and expected outcomes for the pipeline. Performs and documents a deep Exploratory Data Analysis (EDA) on all sources, identifying structure, missing values, and generating insights to guide transformations. | 10% |
| **Data ingestion (Airflow & API)** | Successfully implements three distinct Airflow tasks to automatically extract data at scheduled intervals from a public API, the first delivery dataset, and an additional dataset. | 20% |
| **Data transformation** | Effectively cleans, normalizes, and formats the ingested data. Merges all the data into a single, enriched dataset while handling alignments like date formats and categorical values. | 15% |
| **Data quality (Great Expectations)** | Defines and integrates Great Expectations validation suites directly into the Airflow DAG. Successfully logs validation results and only allows data to load into the final schema if the quality checks pass. | 15% |
| **Data load & modeling** | Properly designs and documents the data model (e.g., star or snowflake schema). Successfully loads the transformed and validated data into the Data Warehouse. | 10% |
| **Visualizations** | Creates meaningful visualizations and dashboards that provide business-relevant insights. Visualizations are built exclusively on top of the validated data stored in the Data Warehouse. | 15% |
| **Deliverables & repository** | Delivers a well-organized GitHub repository containing the code, an updated architecture diagram, and a .gitignore file. Includes a comprehensive README.md that serves as the technical document detailing the ETL pipeline steps, API choice, Airflow DAG design, and key assumptions. | 15% |