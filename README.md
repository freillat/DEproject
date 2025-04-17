# 📊 Data Engineering project on Stock Market Data

## 🧠 Goal

The objective of this project is to build a scalable and reproducible **Data Engineering pipeline** that retrieves historical stock market data, processes it, and generates insightful analytics. It showcases a full modern data stack using cloud-native tools and open-source technologies, designed with modularity and extensibility in mind.

This pipeline performs the following steps:

- Collects historical stock data from the Polygon.io API for a subset of S&P 500 companies.
- Stores the data in **Google Cloud Storage** in an optimized format (Parquet).
- Uses **Apache Spark** to compute analytics such as:
  - Average monthly closing price and volume
  - Best daily returns
  - Worst daily returns
- Loads the processed analytics into **Google BigQuery**.
- Visualizes results in **Looker Studio** for easy consumption and insights.

---

## 🔧 Technologies Used

- 🐳 Docker for containerization  
- ☁️ Google Cloud Platform (GCS, BigQuery)  
- ⚙️ Terraform for Infrastructure as Code  
- 🛠️ Apache Airflow for orchestration  
- ⚡ Apache Spark for analytics  

---

## ✅ Prerequisites

### 1. Google Cloud Setup

- Create a GCP project and note the **Project ID**.
- Create a **Service Account** and assign the following roles:
  - Storage Admin
  - Storage Object Admin
  - Viewer
  - BigQuery Admin
- Download the service account key JSON:
  - Save as:
    - `./gcp-service-account.json` (for Airflow)
    - `./spark/gcp/gcp-service-account.json` (for Spark)
- Update the following files with your Project ID and credentials:
  - `terraform/variables.tf`
  - `spark/app/stock_analytics.py` → Update `BQ_PROJECT`

### 2. Polygon.io API Key

- Used to fetch stock market data.
- A demo API key is included (limited to free-tier usage and will be deactivated in the future).
- You can get your own free key here: [https://polygon.io/pricing](https://polygon.io/pricing)
- Update the key in `dags/fetch_stock_data.py`.

> **Note:** We use a shortened list of S&P 500 tickers to avoid taking too long and comply with the free-tier rate limits. You can find this CSV under `dags/resources/sp500_tickers.csv`.

---

## 🚀 Project Setup

### 1. Deploy GCP Infrastructure with Terraform

```bash
cd terraform
terraform init
terraform apply -var="project_id=your-gcp-project-id" -var="region=us-central1"
```

To clean up resources later:

```bash
terraform destroy
```

### 2. Ingest Data to GCS with Airflow

From the project root:

```bash
docker-compose up --build
```

- Access Airflow at: [http://localhost:8080](http://localhost:8080)  
  Login: `airflow` / `airflow`
- Trigger the DAG `fetch_sp500_data` to download data and store Parquet files in GCS.

Once complete:

```bash
docker-compose down
```

### 3. Run Spark Analytics and Save to BigQuery

From the `spark` directory:

```bash
docker-compose up
```

- This job reads Parquet files from GCS, performs monthly aggregates and return analysis, and stores results in BigQuery.

---

### 4. Dashboard

A simple dashboard was created using **Google Looker Studio**, connected directly to the BigQuery tables generated in the analytics step. It includes:

- Average monthly closing price and volume (per stock)
- Best and worst return days (per stock)

You can view the dashboard here:  
📊 [Looker Studio Report](https://lookerstudio.google.com/reporting/2ad4cf58-09cd-4db1-be3b-71231c12bbcc)

> _Note: The screenshot below is included for reference in case the report link becomes inactive after the free trial period._


![alt text](image.png)


## 🗂️ Project Structure

```
DEproject/
├── gcp-service-account.json
├── docker-compose.yaml
├── Dockerfile
├── README.md
├── terraform/
│   ├── main.tf
│   └── variables.tf
├── dags/
│   ├── fetch_stock_data.py
│   └── resources/
│       └── sp500_tickers.csv
├── spark/
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app/
│   │   └── stock_analytics.py
│   └── gcp/
│       └── gcp-service-account.json
```

---

## Possible Extensions

There are many ways to extend and enhance this project:

### 📈 Data
- Fetch the **entire S&P 500** by handling Polygon API rate limits with time delays or batching.
- Add **fundamental or macroeconomic data** (e.g., P/E ratios, interest rates) for richer analysis.
- Integrate **real-time streaming** updates using Pub/Sub and Dataflow.

### 🧠 Analytics
- Add **technical indicators** (moving averages, RSI, MACD) via Spark.
- Detect **outliers, anomalies**, or regime shifts in stock behavior.
- Perform **correlation analysis** across stocks or sectors.

### 🛠️ Engineering
- Deploy Airflow on **Cloud Composer** for managed orchestration.
- Use **Apache Iceberg or Delta Lake** for versioned data lakes.
- Build a CI/CD pipeline to auto-trigger DAGs on new data.

### 📊 Visualization
- Add filters by sector, time period, or performance metric.
- Use **Metabase or Superset** as alternative BI tools.
- Embed dashboards into web apps or internal portals.

---