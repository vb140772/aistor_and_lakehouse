# BigQuery Dataset and Table for Chicago Taxi Analysis
# Following Google Cloud Terraform Blueprints pattern

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Create dataset for analysis
resource "google_bigquery_dataset" "chicago_taxi_analysis" {
  dataset_id  = "chicago_taxi_analysis_us"
  project     = var.project_id
  location    = var.region
  description = "Dataset for Chicago taxi trips analysis subset"

  labels = {
    purpose = "format-performance-analysis"
  }
}

# Create table with December 2023 subset
resource "google_bigquery_table" "taxi_trips_dec_2023" {
  dataset_id = google_bigquery_dataset.chicago_taxi_analysis.dataset_id
  table_id   = "taxi_trips_dec_2023"
  project    = var.project_id

  description = "Subset of Chicago taxi trips for December 2023"

  # Use query to create table from public dataset
  deletion_protection = false

  labels = {
    purpose = "format-performance-analysis"
    subset  = "dec_2023"
  }
}

# Use null_resource to run the query to populate the table
# since Terraform doesn't support query-based table creation directly
resource "null_resource" "populate_subset_table" {
  provisioner "local-exec" {
    command = <<-EOT
      bq query \
        --use_legacy_sql=false \
        --destination_table=${var.project_id}:${google_bigquery_dataset.chicago_taxi_analysis.dataset_id}.${google_bigquery_table.taxi_trips_dec_2023.table_id} \
        --replace \
        --project_id=${var.project_id} \
        "SELECT * FROM \`bigquery-public-data.chicago_taxi_trips.taxi_trips\` WHERE EXTRACT(YEAR FROM trip_start_timestamp) = 2023 AND EXTRACT(MONTH FROM trip_start_timestamp) = 12"
    EOT
  }

  depends_on = [
    google_bigquery_table.taxi_trips_dec_2023
  ]
}
