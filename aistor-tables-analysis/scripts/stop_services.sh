#!/bin/bash
# Stop MinIO AIStor + Trino services
# Usage: ./stop_services.sh [--clean]

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

# Parse arguments
CLEAN=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --clean)
            CLEAN=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--clean]"
            echo "  --clean: Also remove volumes (deletes all data)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "========================================="
echo "Stopping MinIO AIStor + Trino Services"
echo "========================================="

cd "${DOCKER_DIR}"

if [ "$CLEAN" = true ]; then
    echo "Stopping services and removing volumes..."
    $DOCKER_COMPOSE down -v
    echo "All data has been removed."
else
    echo "Stopping services (data preserved)..."
    $DOCKER_COMPOSE down
    echo ""
    echo "Data is preserved in Docker volumes."
    echo "Use --clean to remove all data."
fi

echo ""
echo "Services stopped."
