#!/bin/bash
# Main orchestration script to run DuckDB analysis in local or GKE mode
# Usage: ./run_analysis.sh [--local|--gke|--help]

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Default mode
MODE="local"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --local)
            MODE="local"
            shift
            ;;
        --gke)
            MODE="gke"
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--local|--gke|--help]"
            echo ""
            echo "Options:"
            echo "  --local    Run analysis locally (default)"
            echo "  --gke      Deploy and run analysis on GKE cluster"
            echo "  --help     Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 --local    # Run locally"
            echo "  $0 --gke      # Deploy to GKE"
            echo "  $0            # Run locally (default)"
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
echo "DuckDB Format Performance Analysis"
echo "========================================="
echo "Mode: ${MODE}"
echo ""

cd "${PROJECT_ROOT}"

if [ "${MODE}" == "local" ]; then
    echo "Running in LOCAL mode..."
    echo ""
    "${SCRIPT_DIR}/run_local_analysis.sh"
elif [ "${MODE}" == "gke" ]; then
    echo "Running in GKE mode..."
    echo ""
    "${SCRIPT_DIR}/deploy_to_gke.sh"
else
    echo "Error: Unknown mode: ${MODE}"
    exit 1
fi
