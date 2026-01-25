# Taxi Data

Shared Chicago taxi trip data for analysis projects.

This folder contains test data used by:
- `aistor-tables-analysis/` - MinIO AIStor Tables integration testing
- `duckdb-format-analysis/` - DuckDB format performance comparison

## Data Formats

| Format | Location | Used By |
|--------|----------|---------|
| Parquet | `parquet/` | Both projects |
| Avro | `avro/` | DuckDB analysis only |

## Prerequisites

Install Python dependencies for data generation:

```bash
cd taxi_data
pip install -r requirements.txt
```

**Dependencies:**
- `numpy`, `pyarrow` - Required for Parquet generation
- `fastavro`, `cramjam` - Required for Avro generation (optional)

## Getting Data

### Option 1: Generate Synthetic Data (Recommended)

Generate realistic synthetic taxi trip data locally. No external dependencies required.

```bash
cd taxi_data

# Generate 1 million rows (default)
./scripts/generate_synthetic.py

# Generate 5 million rows
./scripts/generate_synthetic.py --rows 5

# Generate up to 100 million rows
./scripts/generate_synthetic.py --rows 100

# Generate both Parquet and Avro formats
./scripts/generate_synthetic.py --format both
```

**Options:**
- `--rows` / `-n`: Number of rows in millions (default: 1, max: 100)
- `--format` / `-f`: Output format: `parquet`, `avro`, or `both` (default: both)
- `--output-dir` / `-o`: Output directory (default: current directory)

### Option 2: Download Real Data from GCS

Download actual Chicago taxi trip data from Google Cloud Storage. Requires GCP access.

```bash
cd taxi_data

# Download from default bucket
./scripts/download_from_gcs.sh

# Download from custom bucket
./scripts/download_from_gcs.sh my-bucket-name
```

**Requirements:**
- Google Cloud SDK (`gcloud`) installed
- Access to the GCS bucket

## Data Schema

The taxi trip data includes these columns:

| Column | Type | Description |
|--------|------|-------------|
| `unique_key` | string | Unique trip identifier |
| `taxi_id` | string | Taxi vehicle identifier |
| `trip_start_timestamp` | timestamp | Trip start time |
| `trip_end_timestamp` | timestamp | Trip end time |
| `trip_seconds` | int64 | Trip duration in seconds |
| `trip_miles` | double | Trip distance |
| `fare` | double | Base fare amount |
| `tips` | double | Tip amount |
| `tolls` | double | Toll charges |
| `extras` | double | Extra charges |
| `trip_total` | double | Total amount |
| `payment_type` | string | Payment method |
| `company` | string | Taxi company name |
| `pickup_latitude` | double | Pickup location latitude |
| `pickup_longitude` | double | Pickup location longitude |
| `dropoff_latitude` | double | Dropoff location latitude |
| `dropoff_longitude` | double | Dropoff location longitude |

## File Structure

```
taxi_data/
├── README.md           # This file
├── requirements.txt    # Python dependencies
├── scripts/
│   ├── generate_synthetic.py   # Synthetic data generator
│   └── download_from_gcs.sh    # GCS download script
├── parquet/            # Parquet format files
│   └── *.parquet
└── avro/               # Avro format files (for DuckDB)
    └── *.avro
```
