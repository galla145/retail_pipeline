from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("ProcessedToCurated")
    .config("spark.executor.memory", "512m")
    .config("spark.driver.memory", "512m")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)

df = spark.read.parquet(
    "s3a://retail-pipeline-bucket/processed/retail_sales"
)

df = df.filter(
    (col("Quantity") > 0) &
    (col("UnitPrice") > 0)
)

df = df.repartition(1)

df.write.mode("overwrite").parquet(
    "s3a://retail-pipeline-bucket/curated/retail_sales"
)

spark.stop()
