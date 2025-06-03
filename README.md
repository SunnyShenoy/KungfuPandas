🧬 Databricks Medallion Architecture – Cancer Risk Pipeline
📌 Overview
The Cancer Screening Data pipeline has been built to generate cancer trends among subset of users, measure risks and send Recalls on time so that patients can discuss on ways to prevent the disease or help in early detection for much higher chances of cure. 

The purpose of this project is to build a healthcare application that monitors various cancer risk factors and sends timely reminds from your local healthcare professional which is currently lacking the ability of people of discuss on prevention on time even though their risk factors are quite high.

Inference: The recalls count dashboard gives the count of users that need urgent attention as a preemptive measure. 

This project implements a Medallion Architecture using Databricks Delta Live Tables (DLT) to process healthcare datasets related to cancer risk detection and profiling. The architecture follows a modular structure with Bronze, Silver, and Gold layers to ensure scalable, traceable, and high-quality data transformations.

License: MIT

🗂️ Architecture Layers
🟫 Bronze Layer – Raw Ingestion
Ingests data from external sources (e.g., S3, ADLS, Blob).

Stores raw records with minimal transformations.

Ensures schema inference and resilience (via Auto Loader or batch).

Tables:

kgf_patient_details

kgf_patient_demographics

kgf_patient_cancer_risk

kgf_patient_risk_scores

kgf_patient_cancer_detection

🟪 Silver Layer – Cleaned & Enriched
Performs cleaning, casting, deduplication, and business logic enrichment.

Standardizes schemas and creates derived fields.

Tables:

patient_details

patient_demographics

patient_risk_factors

patient_risk_scores

patient_cancer_detection

🟨 Gold Layer – Aggregated & Ready for Analytics
Joins across domains and aggregates for BI/ML use cases.

Provides dimensions, scores, and grouping for dashboards.

Tables:

patient_details_agg

patient_risk_profile_agg

patient_cancer_detection_metrics

demographics_summary

suburb_patient_distribution

🔧 Pipeline Execution
1. 📦 Full Load vs. Incremental
Use a control notebook with a job_type parameter (prime or delta) to switch between full load and incremental ingestion modes.

python
Copy
Edit
# Pass job_type via widgets
dbutils.widgets.text("job_type", "delta")
2. 🚀 Run Mode
Prime: Drops and recreates schema/tables (use for initial loads).

Delta: Applies incremental logic based on surrogate keys like Patient_ID.

3. ⏱️ Scheduling & Automation
Pipelines can be run via Databricks Workflows, triggered manually or via scheduled jobs.

Audit metrics and expectations are applied to ensure data quality.

📊 Dashboard/Usage
Gold Layer is used to power dashboards for:

Patient risk segmentation

Cancer type likelihood distribution

Detection stage trends

Demographic summaries (by suburb, income, language)

Tools:

Databricks SQL Dashboards

Power BI / Tableau via JDBC

🔒 Data Governance
Access is managed via Unity Catalog.

Tables are governed with row-level and column-level policies if required.

Audit logs and pipeline runs are monitored through Databricks workspace.

📝 Development Notes
Scripts are organized as DLT notebooks (.sql or .py).

Versioning is handled via Git integration with notebooks.

Parameters are injected using dbutils.widgets.