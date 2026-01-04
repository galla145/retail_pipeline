# Cloud Retail Data Pipeline


## Tech Stack
Python | PySpark | SQL | Airflow | AWS S3 | Snowflake | Tableau


## Overview
This project implements an end-to-end ETL pipeline that ingests retail sales data from CSV files and REST APIs, processes it using PySpark, stores it in S3, loads it into Snowflake, and visualizes insights using Tableau.


## Architecture
CSV / API → S3 (Raw) → PySpark → S3 (Processed) → Snowflake → Tableau


## How to Run
1. Upload raw data to S3
2. Run Spark ETL job
3. Trigger Airflow DAG
4. Query data in Snowflake
5. Visualize in Tableau