from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RetailExtract")
    .config("spark.executor.memory", "512m")
    .config("spark.driver.memory", "512m")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)

# IMPORTANT: disable schema inference
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv("s3a://retail-pipeline-bucket/raw/retail_sales_dataset.csv")
)

# Force single partition (CRITICAL)
df = df.repartition(1)

# Write immediately (no actions before this)
df.write.mode("overwrite").parquet(
    "s3a://retail-pipeline-bucket/staging/retail_sales"
)

spark.stop()
