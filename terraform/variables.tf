variable "credentials" {
  description = "My Credentials"
  default     = "C:/Users/freil/Projects/DEproject/gcp-service-account.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}

variable "project_id" {
    default = "stock-data-order66"
}
variable "region" {
  default = "US"
}
variable "bucket_name" {
  default = "sp500-stock-data-bucket"
}