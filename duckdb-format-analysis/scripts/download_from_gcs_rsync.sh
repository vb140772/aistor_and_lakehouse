#!/bin/bash
# Download exported files from GCS to local filesystem using gcloud rsync
# Usage: ./download_from_gcs_rsync.sh [bucket_name]

set -e  # Exit on error

BUCKET_NAME="${1:-bq-export-chicago-taxi-433717}"
LOCAL_BASE_DIR="./data"

# Local directories for each format
AVRO_DIR="${LOCAL_BASE_DIR}/avro"
PARQUET_DIR="${LOCAL_BASE_DIR}/parquet"

# GCS source paths
GCS_AVRO_PATH="gs://${BUCKET_NAME}/exports/dec_2023/avro/"
GCS_PARQUET_PATH="gs://${BUCKET_NAME}/exports/dec_2023/parquet/"

echo "========================================="
echo "Downloading files from GCS to local"
echo "========================================="
echo "Bucket: ${BUCKET_NAME}"
echo "Local base directory: ${LOCAL_BASE_DIR}"
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

# Download Avro files
echo "Downloading Avro files from ${GCS_AVRO_PATH}..."
${RSYNC_CMD} -r "${GCS_AVRO_PATH}" "${AVRO_DIR}/" || {
    echo "Warning: Avro download failed or no files to download."
}

# Download Parquet files
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
echo "Avro files downloaded: ${AVRO_COUNT}"
echo "Parquet files downloaded: ${PARQUET_COUNT}"
echo ""
echo "Files are available at:"
echo "  Avro: ${AVRO_DIR}"
echo "  Parquet: ${PARQUET_DIR}"
