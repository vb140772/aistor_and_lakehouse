#!/bin/bash
# Export BigQuery table to Avro format on GCS
# Usage: ./export_subset_to_avro.sh [table_name] [bucket_name]
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

# Construct GCS path for Avro export
GCS_PATH="gs://${BUCKET_NAME}/exports/dec_2023/avro/${TABLE_BASENAME}_*.avro"

echo "========================================="
echo "Exporting table to Avro format"
echo "========================================="
echo "Table: ${TABLE_NAME}"
echo "Destination: ${GCS_PATH}"
echo ""

# Export to Avro format
echo "Starting export to Avro..."
bq extract \
  --destination_format=AVRO \
  --compression=SNAPPY \
  --use_avro_logical_types \
  "${TABLE_NAME}" \
  "${GCS_PATH}"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Export to Avro completed successfully!"
    echo "Files are available at: ${GCS_PATH}"
else
    echo ""
    echo "✗ Export to Avro failed!"
    exit 1
fi
