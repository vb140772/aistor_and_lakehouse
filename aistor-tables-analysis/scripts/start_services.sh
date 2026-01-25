#!/bin/bash
# Start MinIO AIStor + Trino services
# Usage: ./start_services.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DOCKER_DIR="${PROJECT_ROOT}/docker"

# Detect docker compose command (v2 vs v1)
if docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker compose"
else
    DOCKER_COMPOSE="docker-compose"
fi

echo "========================================="
echo "Starting MinIO AIStor + Trino + Spark Services"
echo "========================================="

# Check if .env file exists
if [ ! -f "${DOCKER_DIR}/.env" ]; then
    echo "Error: ${DOCKER_DIR}/.env not found"
    echo ""
    echo "Please create it from the template:"
    echo "  cp ${DOCKER_DIR}/.env.example ${DOCKER_DIR}/.env"
    echo ""
    echo "Then edit ${DOCKER_DIR}/.env and add your MINIO_LICENSE"
    exit 1
fi

# Check if MINIO_LICENSE is set
if ! grep -q "MINIO_LICENSE=ey" "${DOCKER_DIR}/.env" 2>/dev/null; then
    echo "Warning: MINIO_LICENSE may not be configured in ${DOCKER_DIR}/.env"
    echo "AIStor Tables feature requires a valid license."
    echo ""
fi

# Change to docker directory
cd "${DOCKER_DIR}"

echo "Starting services..."
$DOCKER_COMPOSE up -d

echo ""
echo "Waiting for services to be healthy..."

# Wait for MinIO
echo -n "MinIO: "
for i in {1..30}; do
    if curl -s -f "http://localhost:9000/minio/health/live" >/dev/null 2>&1; then
        echo "ready"
        break
    fi
    echo -n "."
    sleep 2
done

# Wait for Trino
echo -n "Trino: "
for i in {1..30}; do
    if curl -s -f "http://localhost:9999/v1/info" >/dev/null 2>&1; then
        echo "ready"
        break
    fi
    echo -n "."
    sleep 2
done

# Wait for Spark container
echo -n "Spark: "
for i in {1..15}; do
    if docker exec docker-spark-1 /opt/spark/bin/spark-submit --version >/dev/null 2>&1; then
        echo "ready"
        break
    fi
    echo -n "."
    sleep 2
done

echo ""
echo "========================================="
echo "Services are running!"
echo "========================================="
echo ""
echo "MinIO:"
echo "  S3 API:   http://localhost:9000"
echo "  Console:  http://localhost:9001"
echo "  User:     minioadmin"
echo "  Password: minioadmin"
echo ""
echo "Trino:"
echo "  REST API: http://localhost:9999"
echo ""
echo "Spark:"
echo "  Container: docker-spark-1 (Java 17 bundled)"
echo ""
echo "To run analysis:"
echo "  ./scripts/run_trino_analysis.sh"
echo ""
echo "To stop services:"
echo "  ./scripts/stop_services.sh"
