from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Your GCS path
gcs_input_path = "gs://sp500-stock-data-bucket/stocks/"
bq_output_table = "your-project-id.analytics.stock_summary"

spark = SparkSession.builder \
    .appName("Stock Analytics") \
    .getOrCreate()

# Read Parquet from GCS
df = spark.read.parquet(gcs_path)

# Basic Analytics (example: avg close price per stock per day)
summary = df.groupBy("symbol", "date").agg(
    avg("close").alias("avg_close")
)

# Write to BigQuery
summary.write \
    .format("bigquery") \
    .option("table", bq_output_table) \
    .option("writeMethod", "direct") \
    .mode("overwrite") \
    .save()

spark.stop()