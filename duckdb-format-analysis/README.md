# DuckDB Format Performance Analysis

Compare Avro vs Parquet file format performance using DuckDB.

## Overview

This project analyzes the performance differences between **Avro** and **Parquet** file formats for analytical queries using real-world Chicago taxi trip data.

### Key Findings

- **Performance**: Parquet is **150x faster** than Avro for GROUP BY queries
- **Storage**: Parquet files are **3.1x smaller** than Avro (4.2GB vs 13GB)
- **Data**: ~58 million taxi trips across 90 companies (2019-2023)

## Quick Start

### Prerequisites

- Python 3.11+
- Google Cloud SDK (for data download from GCS)

### Installation

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Download data from GCS**:
   ```bash
   ./scripts/download_from_gcs_rsync.sh
   ```

3. **Run analysis**:
   ```bash
   ./scripts/run_analysis.sh --local
   ```

## Project Structure

```
duckdb-format-analysis/
├── analysis/
│   └── run_analysis.py        # Main DuckDB analysis script
├── scripts/
│   ├── run_analysis.sh        # Main entry point
│   ├── run_local_analysis.sh  # Run analysis locally
│   └── download_from_gcs_rsync.sh  # Download data from GCS
├── data/                      # Data directory (Avro + Parquet files)
├── requirements.txt
├── README.md
└── TECHNICAL_OVERVIEW.md
```

## Analysis Query

The benchmark runs the same aggregation query on both formats:

```sql
SELECT 
    company, 
    count(*) as trip_count, 
    sum(fare) as total_fare, 
    avg(fare) as avg_fare
FROM taxi_trips
GROUP BY company
ORDER BY trip_count DESC
```

## Results

| Format | Execution Time | File Size |
|--------|---------------|-----------|
| Parquet | 0.19 seconds | 4.2 GB |
| Avro | 28.3 seconds | 13 GB |

**Parquet is 150x faster** due to columnar storage and efficient compression.

## Scripts Reference

| Script | Purpose |
|--------|---------|
| `run_analysis.sh` | Main orchestration script |
| `run_local_analysis.sh` | Execute DuckDB analysis |
| `download_from_gcs_rsync.sh` | Download data from GCS bucket |

## Configuration

### DuckDB Settings

In `analysis/run_analysis.py`:
- Memory limit: 20GB
- Threads: 4
- Temp directory: `./duckdb_temp`

## Documentation

See [TECHNICAL_OVERVIEW.md](TECHNICAL_OVERVIEW.md) for detailed analysis results and architecture.
