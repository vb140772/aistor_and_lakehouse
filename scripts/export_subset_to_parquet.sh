#!/bin/bash
# Export BigQuery table to Parquet format on GCS
# Usage: ./export_subset_to_parquet.sh [table_name] [bucket_name]
#   table_name: Full table name in format PROJECT_ID:DATASET.TABLE (default: infrastructure-433717.chicago_taxi_analysis_us.taxi_trips_dec_2023)
#   bucket_name: GCS bucket name (default: bq-export-chicago-taxi-433717)

set -e  # Exit on error

# Parse arguments - table name can be in format PROJECT_ID:DATASET.TABLE or DATASET.TABLE
if [ -n "$1" ] && [[ "$1" != gs://* ]] && [[ "$1" != bq-export-* ]]; then
    TABLE_NAME="$1"
    BUCKET_NAME="${2:-bq-export-chicago-taxi-433717}"
else
    TABLE_NAME="${1:-infrastructure-433717.chicago_taxi_analysis_us.taxi_trips_dec_2023}"
    BUCKET_NAME="${2:-bq-export-chicago-taxi-433717}"
fi

# Convert table name format if needed (PROJECT.DATASET.TABLE -> PROJECT:DATASET.TABLE)
if [[ "${TABLE_NAME}" == *.*.* ]] && [[ "${TABLE_NAME}" != *:* ]]; then
    TABLE_NAME=$(echo "${TABLE_NAME}" | sed 's/\./:/' | sed 's/\././')
fi

# Extract table name without project/dataset for file naming
TABLE_BASENAME=$(echo "${TABLE_NAME}" | sed 's/.*\.//')

# Construct GCS path for Parquet export
GCS_PATH="gs://${BUCKET_NAME}/exports/dec_2023/parquet/${TABLE_BASENAME}_*.parquet"

echo "==========================================="
echo "Exporting table to Parquet format"
echo "==========================================="
echo "Table: ${TABLE_NAME}"
echo "Destination: ${GCS_PATH}"
echo ""

# Export to Parquet format
echo "Starting export to Parquet..."
bq extract \
  --destination_format=PARQUET \
  --compression=SNAPPY \
  "${TABLE_NAME}" \
  "${GCS_PATH}"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Export to Parquet completed successfully!"
    echo "Files are available at: ${GCS_PATH}"
else
    echo ""
    echo "✗ Export to Parquet failed!"
    exit 1
fi
