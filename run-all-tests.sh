#!/bin/bash
#
# Unified Test Runner for AIStor Lakehouse
#
# This script orchestrates end-to-end testing of synthetic data generation,
# DuckDB format analysis, and AIStor Tables (Trino/Iceberg) analysis.
#
# Usage:
#   ./run-all-tests.sh --mode host --rows 5
#   ./run-all-tests.sh --mode vm --rows 5
#   ./run-all-tests.sh --mode host --rows 10 --no-cleanup
#
# Options:
#   --mode       Execution mode: 'host' (run directly) or 'vm' (use Lima VM)
#   --rows       Number of rows in millions (default: 5)
#   --no-cleanup Skip cleanup after tests complete
#   --help       Show this help message
#

set -e

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Default values
MODE=""
ROWS=5
CLEANUP=true
VM_NAME="aistor-test"

# Result storage
DUCKDB_AVRO_TIME=""
DUCKDB_PARQUET_TIME=""
TRINO_QUERY_TIME=""
SPARK_INGEST_TIME=""

# ============================================================
# Helper Functions
# ============================================================

print_header() {
    echo ""
    echo -e "${BLUE}============================================================${NC}"
    echo -e "${BOLD}$1${NC}"
    echo -e "${BLUE}============================================================${NC}"
}

print_step() {
    echo -e "${YELLOW}[$1] $2${NC}"
}

print_success() {
    echo -e "${GREEN}$1${NC}"
}

print_error() {
    echo -e "${RED}$1${NC}"
}

print_info() {
    echo -e "${CYAN}$1${NC}"
}

show_help() {
    echo "Unified Test Runner for AIStor Lakehouse"
    echo ""
    echo "Usage: $0 --mode <host|vm> [options]"
    echo ""
    echo "Options:"
    echo "  --mode <host|vm>  Execution mode (required)"
    echo "                    host: Run directly on this machine"
    echo "                    vm:   Run in a dedicated Lima VM"
    echo "  --rows <N>        Number of rows in millions (default: 5)"
    echo "  --no-cleanup      Skip cleanup after tests complete"
    echo "  --help            Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --mode host --rows 5"
    echo "  $0 --mode vm --rows 10 --no-cleanup"
    exit 0
}

# ============================================================
# Argument Parsing
# ============================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            MODE="$2"
            shift 2
            ;;
        --rows)
            ROWS="$2"
            shift 2
            ;;
        --no-cleanup)
            CLEANUP=false
            shift
            ;;
        --help|-h)
            show_help
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Validate mode
if [ -z "$MODE" ]; then
    print_error "Error: --mode is required"
    echo "Use --help for usage information"
    exit 1
fi

if [ "$MODE" != "host" ] && [ "$MODE" != "vm" ]; then
    print_error "Error: --mode must be 'host' or 'vm'"
    exit 1
fi

# ============================================================
# Prerequisites Check
# ============================================================

check_prerequisites() {
    print_header "Checking Prerequisites"
    
    local errors=0
    
    if [ "$MODE" = "host" ]; then
        # Check Python
        if command -v python3 &> /dev/null; then
            PYTHON_VERSION=$(python3 --version 2>&1 | cut -d' ' -f2)
            print_success "  Python: $PYTHON_VERSION"
        else
            print_error "  Python3: NOT FOUND"
            errors=$((errors + 1))
        fi
        
        # Check pip
        if command -v pip3 &> /dev/null || python3 -m pip --version &> /dev/null; then
            print_success "  pip: Available"
        else
            print_error "  pip: NOT FOUND"
            errors=$((errors + 1))
        fi
        
        # Check Docker
        if command -v docker &> /dev/null; then
            DOCKER_VERSION=$(docker --version 2>&1 | cut -d' ' -f3 | tr -d ',')
            print_success "  Docker: $DOCKER_VERSION"
        else
            print_error "  Docker: NOT FOUND"
            errors=$((errors + 1))
        fi
        
        # Check Docker Compose
        if docker compose version &> /dev/null; then
            print_success "  Docker Compose: v2"
        elif command -v docker-compose &> /dev/null; then
            print_success "  Docker Compose: v1"
        else
            print_error "  Docker Compose: NOT FOUND"
            errors=$((errors + 1))
        fi
    else
        # Check limactl
        if command -v limactl &> /dev/null; then
            LIMA_VERSION=$(limactl --version 2>&1 | head -1)
            print_success "  Lima: $LIMA_VERSION"
        else
            print_error "  limactl: NOT FOUND"
            print_error "  Install with: brew install lima"
            errors=$((errors + 1))
        fi
    fi
    
    # Check MinIO license
    ENV_FILE="$SCRIPT_DIR/aistor-tables-analysis/docker/.env"
    if [ -f "$ENV_FILE" ]; then
        if grep -q "MINIO_LICENSE=.*[^[:space:]]" "$ENV_FILE" 2>/dev/null; then
            print_success "  MinIO License: Configured"
        else
            print_error "  MinIO License: NOT SET in $ENV_FILE"
            errors=$((errors + 1))
        fi
    else
        if [ -f "$ENV_FILE.example" ]; then
            print_error "  MinIO License: .env file not found"
            print_info "  Copy .env.example to .env and add MINIO_LICENSE"
            errors=$((errors + 1))
        else
            print_error "  MinIO License: .env.example not found"
            errors=$((errors + 1))
        fi
    fi
    
    if [ $errors -gt 0 ]; then
        print_error ""
        print_error "Found $errors prerequisite error(s). Please fix before continuing."
        exit 1
    fi
    
    print_success ""
    print_success "All prerequisites satisfied!"
}

# ============================================================
# Host Mode Execution
# ============================================================

run_host_mode() {
    print_header "Running Tests on Host"
    print_info "Mode: HOST"
    print_info "Rows: ${ROWS}M"
    print_info "Cleanup: $CLEANUP"
    
    cd "$SCRIPT_DIR"
    
    # Step 1: Setup Python environment
    print_step "1/5" "Setting up Python environment..."
    
    VENV_DIR="$SCRIPT_DIR/.venv"
    if [ ! -d "$VENV_DIR" ]; then
        python3 -m venv "$VENV_DIR"
    fi
    source "$VENV_DIR/bin/activate"
    pip install --upgrade pip -q
    
    # Step 2: Generate synthetic data
    print_step "2/5" "Generating synthetic taxi data (${ROWS}M rows)..."
    
    cd "$SCRIPT_DIR/taxi_data"
    pip install -r requirements.txt -q
    
    # Clean existing data
    rm -f parquet/*.parquet avro/*.avro 2>/dev/null || true
    
    ./scripts/generate_synthetic.py --rows "$ROWS"
    
    PARQUET_COUNT=$(find parquet -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
    AVRO_COUNT=$(find avro -name "*.avro" 2>/dev/null | wc -l | tr -d ' ')
    print_success "  Created: ${PARQUET_COUNT} Parquet files, ${AVRO_COUNT} Avro files"
    
    # Step 3: Run DuckDB analysis
    print_step "3/5" "Running DuckDB format analysis..."
    
    cd "$SCRIPT_DIR/duckdb-format-analysis"
    pip install -r requirements.txt -q
    
    python analysis/run_analysis.py 2>&1 | tee /tmp/duckdb_results.txt
    
    # Extract timing
    DUCKDB_AVRO_TIME=$(grep "Avro execution time:" /tmp/duckdb_results.txt 2>/dev/null | awk '{print $4}' || echo "N/A")
    DUCKDB_PARQUET_TIME=$(grep "Parquet execution time:" /tmp/duckdb_results.txt 2>/dev/null | awk '{print $4}' || echo "N/A")
    
    print_success "  DuckDB analysis complete"
    
    # Step 4: Run AIStor Tables analysis
    print_step "4/5" "Running AIStor Tables analysis..."
    
    cd "$SCRIPT_DIR/aistor-tables-analysis"
    pip install -r requirements.txt -q
    
    # Detect docker compose command
    if docker compose version &> /dev/null; then
        DOCKER_COMPOSE="docker compose"
    else
        DOCKER_COMPOSE="docker-compose"
    fi
    
    # Start Docker services
    echo "  Starting Docker services..."
    cd docker
    $DOCKER_COMPOSE up -d
    cd ..
    
    # Wait for services
    echo "  Waiting for services to be healthy..."
    sleep 15
    
    # Run analysis
    python analysis/run_trino_analysis.py 2>&1 | tee /tmp/trino_results.txt
    
    # Extract timing
    SPARK_INGEST_TIME=$(grep "Data loaded into Iceberg table in" /tmp/trino_results.txt 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' || echo "N/A")
    TRINO_QUERY_TIME=$(grep "Trino/Iceberg execution time:" /tmp/trino_results.txt 2>/dev/null | awk '{print $4}' || echo "N/A")
    
    # Stop Docker services
    echo "  Stopping Docker services..."
    cd docker
    $DOCKER_COMPOSE down
    cd ..
    
    print_success "  AIStor Tables analysis complete"
    
    # Step 5: Show summary
    print_step "5/5" "Test Summary"
    show_summary
    
    # Cleanup
    if [ "$CLEANUP" = true ]; then
        cleanup_host
    fi
    
    deactivate
}

# ============================================================
# VM Mode Execution
# ============================================================

run_vm_mode() {
    print_header "Running Tests in Lima VM"
    print_info "Mode: VM"
    print_info "Rows: ${ROWS}M"
    print_info "Cleanup: $CLEANUP"
    
    cd "$SCRIPT_DIR"
    
    # Step 1: Create and start VM
    print_step "1/5" "Creating and starting Lima VM..."
    
    # Delete existing VM if present
    if limactl list 2>/dev/null | grep -q "$VM_NAME"; then
        echo "  Removing existing VM..."
        limactl delete "$VM_NAME" --force 2>&1 | grep -v "level=warning" || true
    fi
    
    # Create new VM
    echo "  Creating VM (this may take a few minutes)..."
    limactl create --name "$VM_NAME" "$SCRIPT_DIR/test/test-vm.yaml" 2>&1 | grep -v "level=warning" || true
    
    echo "  Starting VM..."
    limactl start "$VM_NAME" 2>&1 | grep -E "(level=info|Downloading|READY)" || true
    
    print_success "  VM is ready"
    
    # Step 2: Copy project files
    print_step "2/5" "Copying project files to VM..."
    
    # Get the VM's home directory
    VM_HOME=$(limactl shell "$VM_NAME" bash -c 'echo $HOME' 2>/dev/null | tail -1)
    VM_PROJECT_DIR="$VM_HOME/src/aistor_and_lakehouse"
    
    limactl shell "$VM_NAME" bash -c "mkdir -p $VM_PROJECT_DIR" 2>&1 | grep -v "level=warning" || true
    limactl shell "$VM_NAME" bash -c "cp -r /Users/vb140772/src/aistor_and_lakehouse/* $VM_PROJECT_DIR/" 2>&1 | grep -v "level=warning" || true
    
    # Copy .env file
    limactl shell "$VM_NAME" bash -c "cp /Users/vb140772/src/aistor_and_lakehouse/aistor-tables-analysis/docker/.env $VM_PROJECT_DIR/aistor-tables-analysis/docker/.env" 2>&1 | grep -v "level=warning" || true
    
    # Make scripts executable
    limactl shell "$VM_NAME" bash -c "chmod +x $VM_PROJECT_DIR/test/*.sh $VM_PROJECT_DIR/taxi_data/scripts/*.py $VM_PROJECT_DIR/aistor-tables-analysis/scripts/*.sh" 2>&1 | grep -v "level=warning" || true
    
    print_success "  Files copied"
    
    # Step 3-4: Run tests inside VM
    print_step "3-4/5" "Running tests inside VM..."
    
    # Run the tests with --rows parameter
    limactl shell "$VM_NAME" bash -c "$VM_PROJECT_DIR/test/run-tests.sh --rows $ROWS" 2>&1 | tee /tmp/vm_results.txt
    
    # Extract timing from VM output (use head -1 to get first match only)
    DUCKDB_AVRO_TIME=$(grep "Avro execution time:" /tmp/vm_results.txt 2>/dev/null | head -1 | awk '{print $4}' || echo "N/A")
    DUCKDB_PARQUET_TIME=$(grep "Parquet execution time:" /tmp/vm_results.txt 2>/dev/null | head -1 | awk '{print $4}' || echo "N/A")
    SPARK_INGEST_TIME=$(grep "Data loaded into Iceberg table in" /tmp/vm_results.txt 2>/dev/null | head -1 | grep -oE '[0-9]+\.[0-9]+' || echo "N/A")
    TRINO_QUERY_TIME=$(grep "Trino/Iceberg execution time:" /tmp/vm_results.txt 2>/dev/null | head -1 | awk '{print $4}' || echo "N/A")
    
    # Step 5: Show summary
    print_step "5/5" "Test Summary"
    show_summary
    
    # Cleanup
    if [ "$CLEANUP" = true ]; then
        cleanup_vm
    fi
}

# ============================================================
# Summary Display
# ============================================================

show_summary() {
    echo ""
    print_header "Performance Summary"
    
    echo ""
    echo -e "${BOLD}Data Generation:${NC}"
    echo "  Rows: ${ROWS}M"
    echo ""
    
    echo -e "${BOLD}DuckDB Format Comparison:${NC}"
    echo "  Avro query time:    ${DUCKDB_AVRO_TIME} seconds"
    echo "  Parquet query time: ${DUCKDB_PARQUET_TIME} seconds"
    
    # Calculate speedup if both values are available
    if [[ "$DUCKDB_AVRO_TIME" != "N/A" && "$DUCKDB_PARQUET_TIME" != "N/A" ]]; then
        SPEEDUP=$(echo "scale=2; $DUCKDB_AVRO_TIME / $DUCKDB_PARQUET_TIME" | bc 2>/dev/null || echo "N/A")
        echo -e "  ${GREEN}Parquet is ${SPEEDUP}x faster${NC}"
    fi
    echo ""
    
    echo -e "${BOLD}AIStor Tables (Trino/Iceberg):${NC}"
    echo "  Spark ingestion:    ${SPARK_INGEST_TIME} seconds"
    echo "  Trino query time:   ${TRINO_QUERY_TIME} seconds"
    echo ""
    
    echo -e "${BOLD}Performance Comparison:${NC}"
    echo "  +-------------------+---------------+"
    echo "  | Engine            | Query Time    |"
    echo "  +-------------------+---------------+"
    printf "  | DuckDB (Parquet)  | %13s |\n" "${DUCKDB_PARQUET_TIME}s"
    printf "  | Trino (Iceberg)   | %13s |\n" "${TRINO_QUERY_TIME}s"
    echo "  +-------------------+---------------+"
    echo ""
    
    # Show top 5 query results from each engine
    echo -e "${BOLD}Top 5 Companies by Trip Count:${NC}"
    echo ""
    
    # Determine which results file to use
    if [ "$MODE" = "host" ]; then
        RESULTS_FILE="/tmp/duckdb_results.txt"
        TRINO_FILE="/tmp/trino_results.txt"
    else
        RESULTS_FILE="/tmp/vm_results.txt"
        TRINO_FILE="/tmp/vm_results.txt"
    fi
    
    echo "  DuckDB (Parquet):"
    if [ -f "$RESULTS_FILE" ]; then
        # Extract data rows from tabulate output (lines with $ for currency values)
        awk '/Parquet Results/,/Total rows:/' "$RESULTS_FILE" 2>/dev/null | \
            grep '\$' | head -5 | \
            while read line; do echo "    $line"; done
    fi
    echo ""
    
    echo "  Trino (Iceberg):"
    if [ -f "$TRINO_FILE" ]; then
        # Extract data rows from tabulate output (lines with $ for currency values)
        awk '/Trino\/Iceberg Results/,/Total rows:/' "$TRINO_FILE" 2>/dev/null | \
            grep '\$' | head -5 | \
            while read line; do echo "    $line"; done
    fi
    echo ""
}

# ============================================================
# Cleanup Functions
# ============================================================

cleanup_host() {
    print_header "Cleaning Up (Host)"
    
    cd "$SCRIPT_DIR"
    
    # Remove generated data
    echo "  Removing generated data..."
    rm -f taxi_data/parquet/*.parquet 2>/dev/null || true
    rm -f taxi_data/avro/*.avro 2>/dev/null || true
    
    # Stop Docker services with volume cleanup
    echo "  Stopping Docker services..."
    cd aistor-tables-analysis/docker
    
    if docker compose version &> /dev/null; then
        docker compose down -v 2>/dev/null || true
    else
        docker-compose down -v 2>/dev/null || true
    fi
    
    cd "$SCRIPT_DIR"
    
    # Remove venv
    echo "  Removing Python virtual environment..."
    rm -rf .venv
    
    print_success "  Cleanup complete"
}

cleanup_vm() {
    print_header "Cleaning Up (VM)"
    
    echo "  Deleting Lima VM..."
    limactl delete "$VM_NAME" --force 2>&1 | grep -v "level=warning" || true
    
    print_success "  VM deleted"
}

# ============================================================
# Main Execution
# ============================================================

print_header "AIStor Lakehouse - Unified Test Runner"
echo ""
echo "Mode:    $MODE"
echo "Rows:    ${ROWS}M"
echo "Cleanup: $CLEANUP"

check_prerequisites

if [ "$MODE" = "host" ]; then
    run_host_mode
else
    run_vm_mode
fi

print_header "All Tests Completed Successfully!"
