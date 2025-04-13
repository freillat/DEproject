provider "google" {
  credentials = file(var.credentials)
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "stock_data_bucket" {
  name     = var.bucket_name
  location = var.region
}

resource "google_bigquery_dataset" "stock_analytics" {
  dataset_id                  = "stock_analytics"
  friendly_name               = "Stock Market Analytics"
  description                 = "Dataset for processed S&P 500 stock data"
  location                    = "US"
  delete_contents_on_destroy = true
}