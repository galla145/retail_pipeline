CREATE OR REPLACE STAGE retail_curated_stage
URL = 's3://retail-pipeline-bucket/curated/'
STORAGE_INTEGRATION = s3_retail_int
FILE_FORMAT = retail_csv_format;
