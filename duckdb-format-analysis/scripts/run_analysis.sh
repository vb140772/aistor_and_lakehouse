#!/bin/bash
# Main orchestration script to run DuckDB analysis locally
# Usage: ./run_analysis.sh [--local|--help]

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --local)
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--local|--help]"
            echo ""
            echo "Options:"
            echo "  --local    Run analysis locally (default)"
            echo "  --help     Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 --local    # Run locally"
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
echo "Mode: local"
echo ""

cd "${PROJECT_ROOT}"

echo "Running in LOCAL mode..."
echo ""
"${SCRIPT_DIR}/run_local_analysis.sh"
