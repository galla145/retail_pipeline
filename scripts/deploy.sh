#!/bin/bash

echo "Starting Retail Data Pipeline Deployment..."

# Activate Python environment
export PATH=$HOME/.local/bin:$PATH

# Upload raw data to S3
aws s3 cp data/retail_data.csv s3://$S3_BUCKET/raw/retail_data.csv

# Run PySpark ETL
python3 etl/extract.py
python3 etl/transform.py
python3 etl/load.py

echo "Pipeline execution completed successfully!"
