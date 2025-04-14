from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, stddev, max as spark_max, min as spark_min,
    from_unixtime, to_date, lag, when, count, month, year, round as spark_round
)
from pyspark.sql.window import Window
from google.cloud import bigquery
# from google.oauth2 import service_account
# import os

# --- Config ---
BQ_PROJECT = "stock-data-order66"  # replace
BQ_DATASET = "stock_analytics"
BQ_LOCATION = "US"  # or "EU" depending on your region
GCP_CREDENTIALS_PATH = "/opt/spark/gcp/gcp-service-account.json"

BUCKET_NAME = "sp500-stock-data-bucket"
BASE_PATH = f"gs://{BUCKET_NAME}/stocks"

# --- Ensure BigQuery Dataset Exists ---``
def ensure_bq_dataset():
    credentials = service_account.Credentials.from_service_account_file(GCP_CREDENTIALS_PATH)
    client = bigquery.Client(credentials=credentials, project=BQ_PROJECT)
    dataset_ref = bigquery.DatasetReference(BQ_PROJECT, BQ_DATASET)

    try:
        client.get_dataset(dataset_ref)
        print(f"BigQuery dataset {BQ_DATASET} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = BQ_LOCATION
        client.create_dataset(dataset)
        print(f"Created BigQuery dataset: {BQ_DATASET}")

# --- Clean DataFrame of NaN / Inf values ---
def clean_df(df):
    from pyspark.sql.functions import isnan
    for c in df.columns:
        if dict(df.dtypes)[c] == 'double':
            df = df.withColumn(c, when(~isnan(col(c)) & col(c).isNotNull(), col(c)).otherwise(None))
    return df

# --- Write to BigQuery ---
def write_to_bq(df, table_name, mode="overwrite"):
    df.write \
        .format("bigquery") \
        .option("table", f"{BQ_PROJECT}:{BQ_DATASET}.{table_name}") \
        .mode(mode) \
        .save()
    print(f"Wrote table: {BQ_DATASET}.{table_name}")        

def main():

    ensure_bq_dataset()

    # Set up Spark session with GCP credentials
    spark = SparkSession.builder \
        .appName("SP500 Stock Analytics") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCP_CREDENTIALS_PATH) \
        .getOrCreate()
    
    # Read all Parquet files matching the pattern
    file_pattern = f"{BASE_PATH}/*.parquet"
    df = spark.read.parquet(file_pattern)

    # Show the schema and a few rows
    df.printSchema()
    df.show(100)

    df = df.withColumn("date", to_date(from_unixtime(col("t") / 1000)))

    # Daily return (safe)
    df = df.withColumn(
        "daily_return", when(col("o").isNotNull() & (col("o") != 0),
                             (col("c") - col("o")) / col("o"))
        .otherwise(None)
    )

    # --- Monthly average close per ticker
    monthly_agg = df.groupBy("ticker", col("date").substr(0, 7).alias("month")) \
        .agg(
            avg("c").alias("avg_close"),
            avg("v").alias("avg_volume")
        )

    # --- Best and Worst Day per Ticker
    daily_return_window = Window.partitionBy("ticker").orderBy(col("daily_return").desc())
    best_day = df.withColumn("rank", spark_max("daily_return").over(daily_return_window)) \
                 .groupBy("ticker").agg(spark_max("daily_return").alias("daily_return")) \
                 .join(df, on=["ticker", "daily_return"]) \
                 .select("ticker", "date", "daily_return")

    worst_day = df.withColumn("rank", spark_min("daily_return").over(daily_return_window)) \
                  .groupBy("ticker").agg(spark_min("daily_return").alias("daily_return")) \
                  .join(df, on=["ticker", "daily_return"]) \
                  .select("ticker", "date", "daily_return")

    # --- YTD Return (from Jan 1 of current year to latest close)
    from pyspark.sql.functions import year, lit

    df_with_year = df.withColumn("year", year("date"))

    jan_1_df = df_with_year.filter((col("date") == lit("2025-01-01"))).select("ticker", col("c").alias("jan_1_close"))
    latest_df = df_with_year.groupBy("ticker", "year").agg(spark_max("date").alias("latest_date")) \
        .join(df_with_year, on=["ticker", "year", "date"]) \
        .select("ticker", "date", col("c").alias("latest_close"))

    ytd = jan_1_df.join(latest_df, on="ticker") \
        .withColumn("ytd_return", (col("latest_close") - col("jan_1_close")) / col("jan_1_close")) \
        .select("ticker", "date", "ytd_return")

    # --- Write to BigQuery (after cleaning)
    write_to_bq(clean_df(monthly_agg), "monthly_aggregates")
    write_to_bq(clean_df(best_day), "best_day_returns")
    write_to_bq(clean_df(worst_day), "worst_day_returns")
    write_to_bq(clean_df(ytd), "ytd_returns")

    spark.stop()

if __name__ == "__main__":
    main()