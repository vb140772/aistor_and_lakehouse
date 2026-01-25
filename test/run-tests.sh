#!/bin/bash
# End-to-end test script for aistor_and_lakehouse
# Run this inside the Lima VM after copying the project
#
# Usage:
#   limactl shell aistor-test ~/src/aistor_and_lakehouse/lima/run-tests.sh
#
# Prerequisites:
#   - Copy project: limactl copy -r ../. aistor-test:~/src/aistor_and_lakehouse/
#   - Add MINIO_LICENSE to docker/.env

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PROJECT_DIR="${HOME}/src/aistor_and_lakehouse"

echo "=============================================="
echo "AIStor Lakehouse - End-to-End Test"
echo "=============================================="
echo ""

# Check if project exists
if [ ! -d "$PROJECT_DIR" ]; then
    echo -e "${RED}Error: Project not found at $PROJECT_DIR${NC}"
    echo "Copy the project first:"
    echo "  limactl copy -r ../. aistor-test:~/src/aistor_and_lakehouse/"
    exit 1
fi

cd "$PROJECT_DIR"

# Step 1: Setup Python virtual environment
echo -e "${YELLOW}[1/5] Setting up Python environment...${NC}"
if [ ! -d "${HOME}/venv" ]; then
    python3.11 -m venv "${HOME}/venv"
fi
source "${HOME}/venv/bin/activate"
pip install --upgrade pip -q

# Step 2: Install taxi_data dependencies and generate data
echo ""
echo -e "${YELLOW}[2/5] Generating synthetic taxi data (5M rows)...${NC}"
cd "$PROJECT_DIR/taxi_data"
pip install -r requirements.txt -q
./scripts/generate_synthetic.py --rows 5

# Verify data was created
PARQUET_COUNT=$(find parquet -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
AVRO_COUNT=$(find avro -name "*.avro" 2>/dev/null | wc -l | tr -d ' ')
echo -e "${GREEN}  Created: ${PARQUET_COUNT} Parquet files, ${AVRO_COUNT} Avro files${NC}"

# Step 3: Run DuckDB analysis
echo ""
echo -e "${YELLOW}[3/5] Running DuckDB format analysis...${NC}"
cd "$PROJECT_DIR/duckdb-format-analysis"
pip install -r requirements.txt -q

# Run analysis and capture output
python analysis/run_analysis.py 2>&1 | tee /tmp/duckdb_results.txt
echo -e "${GREEN}  DuckDB analysis complete${NC}"

# Step 4: Run AIStor Tables analysis
echo ""
echo -e "${YELLOW}[4/5] Running AIStor Tables analysis...${NC}"
cd "$PROJECT_DIR/aistor-tables-analysis"
pip install -r requirements.txt -q

# Check for license
if [ ! -f "docker/.env" ]; then
    if [ -f "docker/.env.example" ]; then
        cp docker/.env.example docker/.env
        echo -e "${RED}  Warning: Created docker/.env from example${NC}"
        echo -e "${RED}  You must add MINIO_LICENSE to docker/.env${NC}"
    fi
fi

if ! grep -q "MINIO_LICENSE=.*[^[:space:]]" docker/.env 2>/dev/null; then
    echo -e "${RED}  Error: MINIO_LICENSE not set in docker/.env${NC}"
    echo "  Please add your MinIO AIStor license and re-run"
    echo ""
    echo "  Skipping AIStor Tables analysis..."
else
    # Start services (use sudo for Docker commands)
    echo "  Starting Docker services..."
    cd docker
    sudo docker-compose up -d
    cd ..
    
    # Wait for services to be ready
    echo "  Waiting for services to be healthy..."
    sleep 15
    
    # Run analysis (pass USE_SUDO=1 to signal subprocess should use sudo)
    USE_SUDO=1 python analysis/run_trino_analysis.py 2>&1 | tee /tmp/trino_results.txt
    
    # Stop services
    echo "  Stopping Docker services..."
    cd docker
    sudo docker-compose down
    cd ..
    
    echo -e "${GREEN}  AIStor Tables analysis complete${NC}"
fi

# Step 5: Summary
echo ""
echo -e "${YELLOW}[5/5] Test Summary${NC}"
echo "=============================================="
echo ""

echo "Data Generation:"
echo "  - Parquet files: ${PARQUET_COUNT}"
echo "  - Avro files: ${AVRO_COUNT}"
echo ""

echo "DuckDB Analysis Results:"
if [ -f /tmp/duckdb_results.txt ]; then
    grep -E "(Parquet|Avro).*seconds" /tmp/duckdb_results.txt 2>/dev/null || echo "  See full output above"
fi
echo ""

echo "AIStor Tables Results:"
if [ -f /tmp/trino_results.txt ]; then
    grep -E "(Spark|Trino|Query).*seconds" /tmp/trino_results.txt 2>/dev/null || echo "  See full output above"
fi

echo ""
echo -e "${GREEN}=============================================="
echo "All tests completed!"
echo "==============================================${NC}"
