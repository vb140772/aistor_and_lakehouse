#!/bin/bash
# Run Trino Iceberg analysis
# Usage: ./run_trino_analysis.sh [--compare-with-duckdb]

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DATA_DIR="${PROJECT_ROOT}/data"
PARQUET_DIR="${DATA_DIR}/parquet"

# Parse arguments
COMPARE_WITH_DUCKDB="false"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --compare-with-duckdb)
            COMPARE_WITH_DUCKDB="true"
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--compare-with-duckdb]"
            echo "  --compare-with-duckdb: Also run DuckDB analysis for comparison"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo "========================================="
echo "Running Trino Iceberg Analysis"
echo "========================================="
echo "Project root: ${PROJECT_ROOT}"
echo "Data directory: ${DATA_DIR}"
echo ""

# Check if Parquet directory exists
if [ ! -d "${PARQUET_DIR}" ]; then
    echo "Parquet directory not found: ${PARQUET_DIR}"
    echo ""
    read -p "Would you like to download data from GCS? (y/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        "${SCRIPT_DIR}/download_from_gcs_rsync.sh" "" "taxi_trips_dec_2023"
        if [ $? -ne 0 ]; then
            echo "Error: Data download failed. Exiting."
            exit 1
        fi
    else
        echo "Please run: ./scripts/download_from_gcs_rsync.sh"
        exit 1
    fi
fi

# Check if files exist
PARQUET_COUNT=$(find "${PARQUET_DIR}" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')

if [ "${PARQUET_COUNT}" -eq 0 ]; then
    echo "Error: No Parquet files found in ${PARQUET_DIR}"
    exit 1
fi

echo "Found ${PARQUET_COUNT} Parquet file(s)"
echo ""

# Check if Trino and MinIO are accessible
# Default Trino port is 9999 (mapped from docker-compose) or 8080 (standard)
TRINO_URI="${TRINO_URI:-http://localhost:9999}"
MINIO_HOST="${MINIO_HOST:-http://localhost:9000}"

echo "Checking Trino connectivity..."
if ! curl -s -f "${TRINO_URI}/v1/info" >/dev/null 2>&1; then
    echo "Warning: Trino may not be accessible at ${TRINO_URI}"
    echo "  Make sure Trino is running (e.g., docker compose up -d trino)"
fi

echo "Checking MinIO connectivity..."
if ! curl -s -f "${MINIO_HOST}/minio/health/live" >/dev/null 2>&1; then
    echo "Warning: MinIO may not be accessible at ${MINIO_HOST}"
    echo "  Make sure MinIO is running (e.g., docker compose up -d minio)"
fi

echo ""

# Change to project root for relative paths
cd "${PROJECT_ROOT}"

# Set environment variables
export EXECUTION_MODE=local
export PARQUET_DIR="${PARQUET_DIR}"
export TRINO_URI="${TRINO_URI}"
export MINIO_HOST="${MINIO_HOST}"
export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
export WAREHOUSE="${WAREHOUSE:-trinotutorial}"
export COMPARE_WITH_DUCKDB="${COMPARE_WITH_DUCKDB}"

# Activate virtual environment if it exists
if [ -d "${PROJECT_ROOT}/.venv" ]; then
    echo "Activating virtual environment..."
    source "${PROJECT_ROOT}/.venv/bin/activate"
fi

# Run the analysis script
echo "Running Trino analysis..."
python3 "${PROJECT_ROOT}/analysis/run_trino_analysis.py"

EXIT_CODE=$?

# Deactivate virtual environment if it was activated
if [ -d "${PROJECT_ROOT}/.venv" ]; then
    deactivate
fi

exit ${EXIT_CODE}
