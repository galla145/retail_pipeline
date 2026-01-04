from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="retail_sales_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["retail", "spark", "snowflake"]
) as dag:

    extract = BashOperator(
        task_id="extract_raw_from_s3",
        bash_command="""
        spark-submit \
        --jars $SPARK_HOME/jars/hadoop-aws-3.3.4.jar,$SPARK_HOME/jars/aws-java-sdk-bundle-1.12.367.jar \
        /home/ec2-user/retail-etl/extract/extract_s3.py
        """
    )

    transform = BashOperator(
        task_id="transform_processed",
        bash_command="""
        spark-submit /home/ec2-user/retail-etl/etl/transform_processed.py
        """
    )

    curate = BashOperator(
        task_id="curate_data",
        bash_command="""
        spark-submit /home/ec2-user/retail-etl/etl/curate_data.py
        """
    )

    load_snowflake = SnowflakeOperator(
        task_id="load_into_snowflake",
        snowflake_conn_id="snowflake_default",
        sql="sql/copy_into_snowflake.sql"
    )

    dq_checks = SnowflakeOperator(
        task_id="snowflake_data_quality",
        snowflake_conn_id="snowflake_default",
        sql="sql/data_quality_checks.sql"
    )

    extract >> transform >> curate >> load_snowflake >> dq_checks
