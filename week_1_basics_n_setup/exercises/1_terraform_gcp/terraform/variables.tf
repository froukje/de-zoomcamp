locals {
	data_lake_bucket = "dtc_lake_bucket"
}

variable "project" {
	description = "Your GCP Project ID"	
}

variable "region" {
	description = "Region for GCP resources. Choose your location: https://cloud.google.com/about/locations"
	default = "europe-west6"
	type = string
}

variable "storage_class" {
	description = "Storage class type for your bucket. Check official docs for more info."
	default = "STANDARD"
}

variable "BQ_DATASET" {
	description = "BigQuery Dataset that raw data (from GCS) will be written to"
	type = string
	default = "trips_data_all"
}

variable "TABLE_NAME" {
	description = "BigQuery Table"
	type = string
	default = "ny_trips"
}
