# terraform resource
terraform {
	required_version = ">=1.0"
	backend "local" {} # can change from "local" to "gcs" (for google) or "s3" (for aws)
	required_providers {
		google = {
			source = "hashicorp/google"
		}
	}
}

provider "google" {
	project = var.project
	region = var.region
	// credentials = file(var.crdentials) # Use this, if you don't want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "google_data_bucket" {
	name	="${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
	location = var.region

	# Optional, but recommended settings:
	storage_class = var.storage_class
	uniform_bucket_level_access = true

	versioning {
		enabled = true
	}

	lifecycle_rule {
		action {
			type = "Delete"
		}
		condition {
			age = 50 // days
		}
	}

	force_destroy = true

}

# DWH
# ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/gigquery_dataset
resource "google_bigquery_dataset" "dataset" {
	dataset_id = var.BQ_DATASET
	project    = var.project
	location   = var.region
}
