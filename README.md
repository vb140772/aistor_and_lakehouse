# AIStor Lakehouse Analytics

Data lakehouse analytics projects demonstrating MinIO AIStor Tables (native Iceberg) integration and file format performance comparison.

## Projects

| Project | Description | Query Engine |
|---------|-------------|--------------|
| [aistor-tables-analysis](./aistor-tables-analysis/README.md) | MinIO AIStor Tables integration with Spark and Trino | Trino + Spark |
| [duckdb-format-analysis](./duckdb-format-analysis/README.md) | Avro vs Parquet format performance comparison | DuckDB |
| [taxi_data](./taxi_data/README.md) | Shared test data (synthetic generator + GCS download) | - |

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

## Quick Start

### 1. Generate Test Data

```bash
cd taxi_data
pip install -r requirements.txt
./scripts/generate_synthetic.py --rows 10  # 10M rows
```

### 2. Run DuckDB Format Analysis

```bash
cd duckdb-format-analysis
pip install -r requirements.txt
./scripts/run_analysis.sh
```

### 3. Run AIStor Tables Analysis

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

## Prerequisites

- Python 3.11+
- Docker and Docker Compose (for AIStor analysis)
- MinIO AIStor license (for AIStor analysis)

## Repository Structure

```
aistor_and_lakehouse/
├── README.md                    # This file
├── taxi_data/                   # Shared test data
│   ├── scripts/
│   │   ├── generate_synthetic.py
│   │   └── download_from_gcs.sh
│   ├── parquet/
│   └── avro/
├── aistor-tables-analysis/      # MinIO AIStor + Trino + Spark
│   ├── analysis/
│   ├── docker/
│   └── scripts/
└── duckdb-format-analysis/      # DuckDB format comparison
    ├── analysis/
    └── scripts/
```

## License

Proprietary - MinIO, Inc.
