from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import time
import os

# API_KEY = os.environ.get("POLYGON_API_KEY")
API_KEY = "KiEBX8MwaypH8pga_nbLsKAWcp9JYam7"
BASE_URL = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2023-01-01/2025-04-14?adjusted=true&sort=asc&limit=50000"

def fetch_stock_data(**kwargs):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from google.cloud import storage

    tickers = pd.read_csv('/opt/airflow/dags/resources/sp500_tickers.csv')['ticker']
    client = storage.Client()
    bucket = client.bucket('sp500-stock-data-bucket')

    for ticker in tickers:
        url = BASE_URL.format(ticker=ticker) + f"&apiKey={API_KEY}"
        resp = requests.get(url).json()
        if "results" in resp:
            df = pd.DataFrame(resp["results"])
            df['ticker'] = ticker
            table = pa.Table.from_pandas(df)
            local_path = f"/tmp/{ticker}.parquet"
            pq.write_table(table, local_path)

            blob = bucket.blob(f"stocks/{ticker}.parquet")
            blob.upload_from_filename(local_path)
        time.sleep(12)  # To avoid hitting the API rate limit

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('fetch_sp500_data',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_stock_data',
        python_callable=fetch_stock_data
    )