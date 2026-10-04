## ACADEMIC PROGRAM: DATA ENGINEERING AND ARTIFICIAL INTELLIGENCE FACULTY OF ENGINEERING AND BASIC SCIENCES

COURSE: ETL (G51)

ETL-Project: First Delivery

You must submit the project in the platform until August 31, 2026 at 23:59.

Teammate

You must collaborate with your colleagues, which means you must form a group of at least 2 people and

no more than 4.

Getting Started

In this first delivery, you will demonstrate your knowledge in data management, data architecture, and

visualizations. Your task will begin with gathering project requirements and evaluating potential data

sources. For this first delivery, the project must be aligned with the United Nations Sustainable

Development Goals (SDGs) 2030. Students are required to select a dataset that addresses one or more

of the 17 global goals—such as Climate Action, Quality Education, or Good Health and Well-being.

Based on this, you will select a suitable dataset (minimum 10,000 rows and at least 10 features) to

conduct exploratory data analysis (EDA) and create insightful visualizations. The Requirements

Gathering and Data Source Evaluation phases must explicitly justify how the chosen data contributes to

understanding or solving an issue within the SDG framework.

You will design a simple data architecture, choose the appropriate technological stack, and migrate the

dataset into a database. Your EDA and visualizations must be based on data stored in the database (not

the original CSV file).

You can start coding from scratch, and the technologies we expect to evaluate are described in the

technologies section.

What is Expected

- Requirements Gathering: Define the objectives and needs of the data analysis task.

- Data Source Evaluation: Justify your choice of dataset based on availability, relevance, structure, and quality. It is expected that you get the CSV file of the dataset you choose and create an ETL application to migrate the data to a relational database.

- Architecture Design: Propose a basic ETL architecture (e.g., data ingestion, storage, transformation, visualization, and Data Warehouse).

- Data Model: Your data model design structures how your data is stored and accessed(e.g., star schema, snowflake schema).

Deadline


- Technology Stack Selection: Justify the tools chosen for each layer of your architecture (e.g., database, visualization, transformation).

- Data Migration: Load the dataset into a relational database (e.g., PostgreSQL, MySQL).

- Visualizations and reports: Build meaningful charts, dashboards and/or reports using the cleaned dataset from the database.

- Final Report: Include a brief technical document that explains:

- The dataset you selected and why

- Requirements and evaluation criteria

- The architecture and tools you chose

- Summary of the EDA process and the visualizations produced

## Technologies

- Python

- Jupiter Notebook

- Database (you choose)

- Visualizations

- Git - GitHub

## Diagram
1-- datase.csv  to raw dataset

2-- raw dataset to dataset EDA to Transformed dataset to dashboard

## Evaluation

| Item | Description | Weight |
| --- | --- | --- |
|   | SDG Alignment & Selection Dataset selection is clearly justified and directly linked to an SDG. | 4% |
| GitHub Repository | Well-organized repository with a logical folder structure and consistent use of informative commits | 6% |
| README & .gitignore | Professional README with execution instructions, stack description, and tool justification. .gitignore is correctly configured to exclude binaries/caches. | 10% |


|   | Data Migration to Database Successful migration from CSV to a relational database (e.g., PostgreSQL, MySQL) while preserving data integrity. | 10% |
| --- | --- | --- |
| Exploratory Data Analysis (EDA) | Deep EDA identifying trends, null values, and data quality (min. 10k rows/10 features). | 20% |
| Data Extraction from DB | Visualizations and analysis are powered exclusively by SQL queries or connections to the database. | 10% |
| Visualizations (Charts/Dashboards) | Meaningful charts or dashboards that clearly answer the project objectives and requirements. | 20% |
| Technical Report | Document detailing the architecture (Star/Snowflake), technology stack, and source evaluation criteria | 20% |
