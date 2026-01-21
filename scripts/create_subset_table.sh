#!/bin/bash
# Create BigQuery table with December 2023 subset from public dataset
# Usage: ./create_subset_table.sh

set -e  # Exit on error

PROJECT_ID="infrastructure-433717"
DATASET="chicago_taxi_analysis_us"
TABLE="taxi_trips_dec_2023"

echo "========================================="
echo "Creating BigQuery subset table"
echo "========================================="
echo "Project: ${PROJECT_ID}"
echo "Dataset: ${DATASET}"
echo "Table: ${TABLE}"
echo ""

# Set the project
gcloud config set project ${PROJECT_ID}

# Create dataset if it doesn't exist
echo "Creating dataset if it doesn't exist..."
bq --project_id=${PROJECT_ID} mk --dataset --location=us-central1 ${PROJECT_ID}:${DATASET} 2>/dev/null || {
    echo "Dataset already exists or creation failed (continuing...)"
}

# Create table with December 2023 subset
# Use CREATE TABLE AS SELECT syntax
echo "Creating table with December 2023 subset..."
echo "This may take a few minutes..."
bq query \
  --use_legacy_sql=false \
  --project_id=${PROJECT_ID} \
  "CREATE OR REPLACE TABLE \`${PROJECT_ID}.${DATASET}.${TABLE}\` AS SELECT * FROM \`bigquery-public-data.chicago_taxi_trips.taxi_trips\` WHERE EXTRACT(YEAR FROM trip_start_timestamp) = 2023 AND EXTRACT(MONTH FROM trip_start_timestamp) = 12"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Table created successfully!"
    echo "Table: ${PROJECT_ID}:${DATASET}.${TABLE}"
    
    # Get row count
    ROW_COUNT=$(bq query --use_legacy_sql=false --format=csv --project_id=${PROJECT_ID} "SELECT COUNT(*) as cnt FROM \`${PROJECT_ID}.${DATASET}.${TABLE}\`" | tail -n 1)
    echo "Row count: ${ROW_COUNT}"
else
    echo ""
    echo "✗ Table creation failed!"
    exit 1
fi
