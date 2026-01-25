#!/bin/bash
# Download Chicago taxi trip data from GCS
# Usage: ./download_from_gcs.sh [bucket_name]
#
# Downloads both Avro and Parquet formats for use by:
# - aistor-tables-analysis (Parquet)
# - duckdb-format-analysis (Avro + Parquet)

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TAXI_DATA_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

BUCKET_NAME="${1:-bq-export-chicago-taxi-433717}"

# Output directories
AVRO_DIR="${TAXI_DATA_DIR}/avro"
PARQUET_DIR="${TAXI_DATA_DIR}/parquet"

# GCS source paths
GCS_AVRO_PATH="gs://${BUCKET_NAME}/exports/dec_2023/avro/"
GCS_PARQUET_PATH="gs://${BUCKET_NAME}/exports/dec_2023/parquet/"

echo "========================================="
echo "Chicago Taxi Data - GCS Download"
echo "========================================="
echo "Bucket: ${BUCKET_NAME}"
echo "Output: ${TAXI_DATA_DIR}"
echo ""

# Create local directories
mkdir -p "${AVRO_DIR}"
mkdir -p "${PARQUET_DIR}"

# Check if gcloud storage is available (preferred), otherwise use gsutil
if command -v gcloud &> /dev/null && gcloud storage --help &> /dev/null; then
    RSYNC_CMD="gcloud storage rsync"
    echo "Using gcloud storage rsync..."
else
    RSYNC_CMD="gsutil -m rsync"
    echo "Using gsutil rsync..."
fi

echo ""

# Download Avro files
echo "Downloading Avro files from ${GCS_AVRO_PATH}..."
${RSYNC_CMD} -r "${GCS_AVRO_PATH}" "${AVRO_DIR}/" || {
    echo "Warning: Avro download failed or no files to download."
}

# Download Parquet files
echo ""
echo "Downloading Parquet files from ${GCS_PARQUET_PATH}..."
${RSYNC_CMD} -r "${GCS_PARQUET_PATH}" "${PARQUET_DIR}/" || {
    echo "Warning: Parquet download failed or no files to download."
}

# Count downloaded files
AVRO_COUNT=$(find "${AVRO_DIR}" -name "*.avro" 2>/dev/null | wc -l | tr -d ' ')
PARQUET_COUNT=$(find "${PARQUET_DIR}" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')

echo ""
echo "========================================="
echo "Download Summary"
echo "========================================="
echo "Avro files:    ${AVRO_COUNT}"
echo "Parquet files: ${PARQUET_COUNT}"
echo ""
echo "Location: ${TAXI_DATA_DIR}"
echo "  Avro:    ${AVRO_DIR}"
echo "  Parquet: ${PARQUET_DIR}"
