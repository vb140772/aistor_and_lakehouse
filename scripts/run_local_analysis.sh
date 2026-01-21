#!/bin/bash
# Run DuckDB analysis locally
# Usage: ./run_local_analysis.sh

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DATA_DIR="${PROJECT_ROOT}/data"
AVRO_DIR="${DATA_DIR}/avro"
PARQUET_DIR="${DATA_DIR}/parquet"

echo "========================================="
echo "Running DuckDB Analysis (Local Mode)"
echo "========================================="
echo "Project root: ${PROJECT_ROOT}"
echo "Data directory: ${DATA_DIR}"
echo ""

# Check if data directories exist
if [ ! -d "${AVRO_DIR}" ] || [ ! -d "${PARQUET_DIR}" ]; then
    echo "Data directories not found!"
    echo "Avro directory: ${AVRO_DIR}"
    echo "Parquet directory: ${PARQUET_DIR}"
    echo ""
    echo "Would you like to download data from GCS? (y/n)"
    read -r response
    if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        echo "Downloading data from GCS..."
        "${SCRIPT_DIR}/download_from_gcs_rsync.sh"
    else
        echo "Please run: ./scripts/download_from_gcs_rsync.sh"
        exit 1
    fi
fi

# Check if files exist
AVRO_COUNT=$(find "${AVRO_DIR}" -name "*.avro" 2>/dev/null | wc -l | tr -d ' ')
PARQUET_COUNT=$(find "${PARQUET_DIR}" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')

if [ "${AVRO_COUNT}" -eq 0 ]; then
    echo "Error: No Avro files found in ${AVRO_DIR}"
    exit 1
fi

if [ "${PARQUET_COUNT}" -eq 0 ]; then
    echo "Error: No Parquet files found in ${PARQUET_DIR}"
    exit 1
fi

echo "Found ${AVRO_COUNT} Avro file(s) and ${PARQUET_COUNT} Parquet file(s)"
echo ""

# Change to project root for relative paths
cd "${PROJECT_ROOT}"

# Set execution mode and run analysis
export EXECUTION_MODE=local
python3 "${PROJECT_ROOT}/analysis/run_analysis.py"
