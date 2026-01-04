Retail Sales Airflow Pipeline

DAG:
- retail_sales_etl_pipeline

Flow:
S3 Raw → Spark Extract
→ Spark Transform
→ Spark Curate
→ Snowflake Load
→ Tableau Dashboard
