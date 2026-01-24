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

# Check Java version - Spark requires Java 8-21 (Java 24+ is incompatible)
if [ -d "/opt/homebrew/opt/openjdk@21" ]; then
    export JAVA_HOME=/opt/homebrew/opt/openjdk@21
    export PATH="$JAVA_HOME/bin:$PATH"
    echo "Using Java 21 for Spark compatibility"
elif [ -d "/usr/lib/jvm/java-21-openjdk" ]; then
    export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
    export PATH="$JAVA_HOME/bin:$PATH"
    echo "Using Java 21 for Spark compatibility"
else
    JAVA_VERSION=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$JAVA_VERSION" -ge 24 ] 2>/dev/null; then
        echo "Warning: Java $JAVA_VERSION detected. Spark requires Java 8-21."
        echo "  Install Java 21: brew install openjdk@21"
        echo "  Then run with: JAVA_HOME=/opt/homebrew/opt/openjdk@21 $0"
    fi
fi
echo ""

# Check if Trino and MinIO are accessible
# Default Trino port is 9999 (mapped from docker-compose) or 8080 (standard)
TRINO_URI="${TRINO_URI:-http://localhost:9999}"
MINIO_HOST="${MINIO_HOST:-http://localhost:9000}"

SERVICES_MISSING=false

echo "Checking service connectivity..."
echo ""

echo -n "MinIO (${MINIO_HOST}): "
if curl -s -f "${MINIO_HOST}/minio/health/live" >/dev/null 2>&1; then
    echo "OK"
else
    echo "NOT AVAILABLE"
    SERVICES_MISSING=true
fi

echo -n "Trino (${TRINO_URI}): "
if curl -s -f "${TRINO_URI}/v1/info" >/dev/null 2>&1; then
    echo "OK"
else
    echo "NOT AVAILABLE"
    SERVICES_MISSING=true
fi

echo ""

if [ "$SERVICES_MISSING" = true ]; then
    echo "========================================="
    echo "ERROR: Required services are not running"
    echo "========================================="
    echo ""
    echo "Please start the services first:"
    echo "  ./scripts/start_services.sh"
    echo ""
    echo "Or manually with Docker Compose:"
    echo "  cd docker && docker compose up -d"
    echo ""
    exit 1
fi

echo "All services are available."
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
