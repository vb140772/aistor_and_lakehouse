# AIStor Lakehouse Analytics

Data lakehouse analytics projects demonstrating MinIO AIStor Tables (native Iceberg) integration and file format performance comparison.

## Projects

| Project | Description | Query Engine |
|---------|-------------|--------------|
| [aistor-tables-analysis](./aistor-tables-analysis/README.md) | MinIO AIStor Tables integration with Spark and Trino | Trino + Spark |
| [duckdb-format-analysis](./duckdb-format-analysis/README.md) | Avro vs Parquet format performance comparison | DuckDB |
| [taxi_data](./taxi_data/README.md) | Shared test data (synthetic generator + BigQuery export) | - |

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        taxi_data/                               │
│              (Shared Parquet & Avro test data)                  │
└─────────────────────┬───────────────────────┬───────────────────┘
                      │                       │
                      ▼                       ▼
┌─────────────────────────────────┐ ┌─────────────────────────────┐
│   aistor-tables-analysis/       │ │   duckdb-format-analysis/   │
│                                 │ │                             │
│   MinIO AIStor + Iceberg        │ │   DuckDB Engine             │
│   ├── Spark (data ingestion)    │ │   ├── Parquet queries       │
│   └── Trino (SQL analytics)     │ │   └── Avro queries          │
│                                 │ │                             │
│   Docker: MinIO, Trino, Spark   │ │   Local Python              │
└─────────────────────────────────┘ └─────────────────────────────┘
```

## Setup

### 1. Configure MinIO License (Required for AIStor Analysis)

```bash
# Copy the example configuration
cp aistor-tables-analysis/docker/.env.example aistor-tables-analysis/docker/.env

# Edit .env and add your license key (get one from https://subnet.min.io)
```

The `.env` file contains:

| Variable | Description |
|----------|-------------|
| `MINIO_LICENSE` | Your MinIO AIStor license key (required) |
| `MINIO_TEST_IMAGE` | AIStor Docker image version |
| `TRINO_IMAGE` | Trino query engine version |

**Note:** The `.env` file is gitignored to protect your license key.

### 2. Prerequisites

- Python 3.8+ (3.11+ recommended)
- Docker and Docker Compose
- Lima (`brew install lima`) - optional, for VM mode

## Quick Start

### Unified Test Runner (Recommended)

Run all tests with a single command:

```bash
# Run on host machine (requires Docker)
./run-all-tests.sh --mode host --rows 5

# Run in isolated Lima VM (requires limactl)
./run-all-tests.sh --mode vm --rows 5

# Options:
#   --mode host|vm    Execution mode (required)
#   --rows N          Millions of rows to generate (default: 5)
#   --no-cleanup      Keep data after tests complete
```

The unified test runner will:
1. Generate synthetic taxi data
2. Run DuckDB format analysis (Avro vs Parquet)
3. Run AIStor Tables analysis (Spark ingestion + Trino queries)
4. Display performance comparison summary
5. Clean up all data and containers

### Manual Execution

#### 1. Get Test Data

```bash
cd taxi_data
pip install -r requirements.txt

# Option A: Generate synthetic data (fast, no GCP required)
./scripts/generate_synthetic.py --rows 10  # 10M rows

# Option B: Export real data from BigQuery (requires GCP project)
./scripts/download_from_bq.py --project my-gcp-project --years 5
```

#### 2. Run DuckDB Format Analysis

```bash
cd duckdb-format-analysis
pip install -r requirements.txt
./scripts/run_analysis.sh
```

#### 3. Run AIStor Tables Analysis

```bash
cd aistor-tables-analysis
pip install -r requirements.txt
cp docker/.env.example docker/.env
# Edit docker/.env and add MINIO_LICENSE
./scripts/start_services.sh
./scripts/run_trino_analysis.sh
```

## Key Findings

### File Format Comparison (DuckDB)

| Format | Query Time | Storage Size | Speedup |
|--------|------------|--------------|---------|
| Parquet | 0.19s | 4.2 GB | **150x faster** |
| Avro | 28.3s | 13 GB | Baseline |

### AIStor Tables (Trino + Iceberg)

- Spark ingestion: ~4s for 10M rows
- Trino query: ~1s for aggregation
- Benefits: ACID transactions, schema evolution, time travel

## Repository Structure

```
aistor_and_lakehouse/
├── README.md                    # This file
├── run-all-tests.sh             # Unified test runner (host or VM mode)
├── taxi_data/                   # Shared test data
│   ├── scripts/
│   │   ├── generate_synthetic.py
│   │   ├── download_from_bq.py
│   │   └── download_from_gcs.sh
│   ├── parquet/
│   └── avro/
├── aistor-tables-analysis/      # MinIO AIStor + Trino + Spark
│   ├── analysis/
│   ├── docker/
│   └── scripts/
├── duckdb-format-analysis/      # DuckDB format comparison
│   ├── analysis/
│   └── scripts/
└── test/                        # Lima VM test configuration
    ├── test-vm.yaml
    └── run-tests.sh
```

## License

Proprietary - MinIO, Inc.
