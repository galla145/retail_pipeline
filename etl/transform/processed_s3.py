from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_timestamp

spark = (
    SparkSession.builder
    .appName("StagingToProcessed")
    .config("spark.executor.memory", "512m")
    .config("spark.driver.memory", "512m")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)

df = spark.read.parquet(
    "s3a://retail-pipeline-bucket/staging/retail_sales"
)

df = (
    df
    .withColumn("InvoiceNo", trim(col("InvoiceNo")))
    .withColumn("Country", trim(col("Country")))
    .withColumn(
        "InvoiceDate",
        to_timestamp(col("InvoiceDate"), "MM/dd/yyyy HH:mm")
    )
    .withColumn(
        "total_amount",
        col("Quantity").cast("int") * col("UnitPrice").cast("double")
    )
)

df = df.repartition(1)

df.write.mode("overwrite").parquet(
    "s3a://retail-pipeline-bucket/processed/retail_sales"
)

spark.stop()
