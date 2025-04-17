from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, avg, stddev, max as spark_max, min as spark_min,
    from_unixtime, to_date, lag, when, count, month, year, lit, round as spark_round
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, LongType, StringType
from google.cloud import bigquery, storage
from google.oauth2 import service_account
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# --- Config ---
BQ_PROJECT = "stock-data-order66"  # replace
BQ_DATASET = "stock_analytics"
BQ_LOCATION = "US"
GCP_CREDENTIALS_PATH = "/opt/spark/gcp/gcp-service-account.json"
BUCKET_NAME = "sp500-stock-data-bucket"
BASE_PATH = f"gs://{BUCKET_NAME}/stocks"

def cast_stock_df(df: DataFrame) -> DataFrame:
    return df.select(
        col("v").cast(DoubleType()).alias("v"),
        col("vw").cast(DoubleType()).alias("vw"),
        col("o").cast(DoubleType()).alias("o"),
        col("c").cast(DoubleType()).alias("c"),
        col("h").cast(DoubleType()).alias("h"),
        col("l").cast(DoubleType()).alias("l"),
        col("t").cast(LongType()).alias("t"),
        col("n").cast(LongType()).alias("n"),
        col("ticker").cast(StringType()).alias("ticker")
    )

def list_parquet_files(bucket_name: str, prefix: str) -> list:
    prefix = prefix.rstrip("/") + "/"  # ensure it's a folder-style path
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    return [f"gs://{bucket_name}/{blob.name}" for blob in blobs if blob.name.endswith(".parquet")]

# def read_all_stock_data(spark, bucket_name="sp500-stock-data-bucket", folder="stocks"):
#     folder = folder.replace("*", "").rstrip("/")
#     files = list_parquet_files(bucket_name, folder)
#     all_dfs = []
#     skipped_files = []

#     for file_path in files:
#         try:
#             df = spark.read.option("mergeSchema", "false").parquet(file_path)
#             casted_df = cast_stock_df(df)
#             all_dfs.append(casted_df)
#         except Exception as e:
#             print(f"⚠️ Skipping file {file_path} due to error: {e}")
#             skipped_files.append((file_path, str(e)))

#     if skipped_files:
#         print("\n📄 Skipped files summary:")
#         for path, err in skipped_files:
#             print(f"  - {path} → {err}")

#     if not all_dfs:
#         return None

#     # ✅ Fix: union using a loop
#     result_df = all_dfs[0]
#     for df in all_dfs[1:]:
#         result_df = result_df.unionByName(df, allowMissingColumns=True)

#     return result_df

def read_all_stock_data_optimized(spark: SparkSession, base_path: str) -> DataFrame:
    logger.info(f"Reading all stock data from {base_path}")
    df = spark.read.option("mergeSchema", "false").parquet(base_path)
    return cast_stock_df(df)

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
    try:
        df.coalesce(1).write \
            .format("bigquery") \
            .option("temporaryGcsBucket", BUCKET_NAME) \
            .option("table", f"{BQ_PROJECT}:{BQ_DATASET}.{table_name}") \
            .mode(mode) \
            .save()
        logger.info("✅ Wrote table: %s.%s", BQ_DATASET, table_name)
    except Exception as e:
        logger.exception("❌ Failed to write table: %s", table_name)    

def main():

    ensure_bq_dataset()

    # Set up Spark session with GCP credentials
    spark = SparkSession.builder \
        .appName("SP500 Stock Analytics") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCP_CREDENTIALS_PATH) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.gs.project.id", BQ_PROJECT) \
        .config("spark.hadoop.fs.gs.bucket", BUCKET_NAME) \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.memoryOverhead", "4g") \
        .config("spark.num.executors", "5") \
        .config("spark.driver.memory", "8g") \
        .config("spark.driver.memoryOverhead", "4g") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.fs.gs.outputstream.type", "SYNCABLE_COMPOSITE") \
        .config("spark.hadoop.fs.gs.status.parallel.enable", "false") \
        .getOrCreate()
    
    # Read all Parquet files
    # df = read_all_stock_data(spark, bucket_name=BUCKET_NAME, folder="stocks")
    df = read_all_stock_data_optimized(spark, BASE_PATH)

    # Show the schema and a few rows
    df.printSchema()
    df.show(10)

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

    # # --- YTD Return (from Jan 1 of current year to latest close)

    # # Add year column to base DataFrame
    # df_with_year = df.withColumn("year", year("date"))

    # # ---- Jan 1 close price per ticker ----
    # jan_df = df_with_year.filter(col("date") == lit("2025-01-01")).alias("jan")

    # # ---- Latest close price per ticker ----
    # # Step 1: get the latest date per ticker/year
    # latest_date_df = df_with_year.groupBy("ticker", "year") \
    #     .agg(spark_max("date").alias("latest_date")).alias("latest")

    # # Step 2: join to get the latest close
    # df_alias = df_with_year.alias("df")

    # latest_df = latest_date_df.join(
    #     df_alias,
    #     (df_alias["ticker"] == latest_date_df["ticker"]) &
    #     (df_alias["year"] == latest_date_df["year"]) &
    #     (df_alias["date"] == latest_date_df["latest_date"])
    # ).select(
    #     df_alias["ticker"].alias("ticker"),
    #     df_alias["date"].alias("latest_date"),
    #     df_alias["c"].alias("latest_close")
    # ).alias("latest")

    # # ---- Join Jan and latest to compute YTD return ----
    # ytd_df = jan_df.join(
    #     latest_df,
    #     jan_df["ticker"] == latest_df["ticker"]
    # ).select(
    #     jan_df["ticker"],
    #     latest_df["latest_date"].alias("date"),
    #     ((latest_df["latest_close"] - jan_df["c"]) / jan_df["c"]).alias("ytd_return")
    # )

    # --- Write to BigQuery (after cleaning)
    write_to_bq(clean_df(monthly_agg), "monthly_aggregates")
    write_to_bq(clean_df(best_day), "best_day_returns")
    write_to_bq(clean_df(worst_day), "worst_day_returns")
    # write_to_bq(clean_df(ytd_df), "ytd_returns")

    spark.stop()

if __name__ == "__main__":
    main()