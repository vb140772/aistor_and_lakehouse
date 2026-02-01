# Iceberg Internals Lab

An interactive lab demonstrating how Apache Iceberg table operations (INSERT, DELETE, UPDATE) are reflected in catalog metadata and data files using MinIO AIStor.

## Learning Objectives

By completing this lab, you will understand:

1. **Iceberg Table Structure** - How tables are organized with metadata.json, manifests, and data files
2. **Atomic Commits** - How INSERT operations create new snapshots and data files
3. **Delete Operations** - How Iceberg handles deletes (copy-on-write vs merge-on-read)
4. **Update Operations** - How updates work as DELETE + INSERT
5. **Time Travel** - How to query historical data using snapshots

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       MinIO AIStor                               │
│                   (S3 Storage + Iceberg Catalog)                 │
├─────────────────────────────────────────────────────────────────┤
│  Warehouse: iceberglab                                          │
│  └── Namespace: demo                                             │
│       └── Table: employees                                       │
│            ├── metadata/                                         │
│            │   ├── metadata.json  (table metadata)               │
│            │   └── snap-*.avro    (manifest lists)               │
│            └── data/                                             │
│                └── *.parquet      (data files)                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                          Trino                                   │
│                    (SQL Query Engine)                            │
│  - CREATE TABLE, INSERT, DELETE, UPDATE                          │
│  - Time travel queries (FOR VERSION AS OF)                       │
└─────────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- MinIO AIStor license (get from https://subnet.min.io)

## Setup

### 1. Configure License

```bash
cd iceberg-internals-lab
cp docker/.env.example docker/.env
# Edit docker/.env and add your MINIO_LICENSE
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Start Services

```bash
./scripts/start_services.sh
```

This starts:
- **MinIO AIStor** - S3 storage with native Iceberg catalog (ports 9000, 9001)
- **Trino** - SQL query engine with Iceberg connector (port 9999)

## Run the Lab

```bash
# Show help (default when run without --run)
python lab/run_lab.py

# Run the lab
python lab/run_lab.py --run
```

The lab is interactive - press any key to advance through each step.

### Output Modes

Use `--mode` to reduce output and focus on specific aspects:

| Mode | Output |
|------|--------|
| `full` (default) | All output - storage changes and SQL verification |
| `storage` | Object storage view only: catalog structure, metadata, manifests, MinIO files |
| `sql` | Trino view only: information_schema, metadata tables ($snapshots, $files, etc.) |

```bash
# Object storage focus (manifest content, MinIO files)
python lab/run_lab.py --run --mode storage

# SQL / information schema focus
python lab/run_lab.py --run --mode sql
```

## Lab Steps

### Step 1: Create Table

Creates an `employees` table and examines:
- Catalog structure (warehouse → namespace → table)
- Initial metadata.json file
- Empty table (no data files yet)

### Step 2: Insert 1000 Rows

Inserts 1000 synthetic employee records and shows:
- New Parquet data files created
- New snapshot with INSERT operation
- Manifest files tracking data files

### Step 3: Delete Rows

Deletes all Sales department employees (~100 rows):
- New snapshot with DELETE operation
- Copy-on-write: data files rewritten without deleted rows
- Old files preserved for time travel

### Step 4: Update Rows

Gives everyone a 10% raise:
- UPDATE = DELETE + INSERT
- New data files with updated values
- Old files still accessible

### Step 5: Time Travel

Queries historical data:
- View all snapshots
- Query data at previous snapshots
- Compare current vs historical state

## Inspection Utilities

The lab includes utility scripts to inspect Avro and Parquet files directly. These are useful for understanding the internal structure of Iceberg metadata and data files.

### When to Use

- **Inspect Avro files** (`inspect_avro.py`): When examining Iceberg manifest-list files (`snap-*.avro`) or manifest files (`*-m*.avro`) to see:
  - Schema structure
  - Manifest entries (file paths, row counts, partition info)
  - Delete file tracking

- **Inspect Parquet files** (`inspect_parquet.py`): When examining Iceberg data files (`*.parquet`) to see:
  - Column schema and types
  - Row counts and file statistics
  - Compression information
  - Sample data records

### Usage Examples

#### Inspect Avro Files (Manifests)

```bash
# Inspect a manifest-list file
mc cat local/iceberglab/<table-uuid>/metadata/snap-*.avro | \
  python scripts/inspect_avro.py

# Show sample manifest entries
mc cat local/iceberglab/<table-uuid>/metadata/snap-*.avro | \
  python scripts/inspect_avro.py --show-records

# Use JSON format instead of YAML
mc cat local/iceberglab/<table-uuid>/metadata/snap-*.avro | \
  python scripts/inspect_avro.py --format json
```

#### Inspect Parquet Files (Data Files)

```bash
# Inspect a data file
mc cat local/iceberglab/<table-uuid>/data/*.parquet | \
  python scripts/inspect_parquet.py

# Show sample records from the file
mc cat local/iceberglab/<table-uuid>/data/*.parquet | \
  python scripts/inspect_parquet.py --show-records

# Show more sample records
mc cat local/iceberglab/<table-uuid>/data/*.parquet | \
  python scripts/inspect_parquet.py --show-records --max-records 10
```

#### Finding Files to Inspect

After running the lab, you can find files to inspect:

```bash
# List all files in the warehouse
mc ls -r local/iceberglab/

# Find manifest-list files
mc find local/iceberglab --name "snap-*.avro"

# Find data files
mc find local/iceberglab --name "*.parquet"
```

### Output Format

Both utilities use **YAML format by default** (more compact and readable) with syntax highlighting:
- Schema information with types and metadata
- File statistics (size, row counts, compression)
- Sample records (when using `--show-records`)

Use `--format json` to switch to JSON output if preferred.

### Requirements

The inspection utilities require:
- `fastavro` (for Avro files) - already in `requirements.txt`
- `pyarrow` (for Parquet files) - already in `requirements.txt`
- `pyyaml` (for YAML output) - already in `requirements.txt`

Install all dependencies:
```bash
pip install -r requirements.txt
```

## Cleanup

```bash
# Stop services (preserve data)
./scripts/stop_services.sh

# Stop services and remove all data
./scripts/stop_services.sh --clean
```

## Key Concepts

### Iceberg File Structure

```
table-location/
├── metadata/
│   ├── v1.metadata.json      # Initial table metadata
│   ├── v2.metadata.json      # After INSERT
│   ├── v3.metadata.json      # After DELETE
│   ├── snap-123-*.avro       # Manifest list for snapshot
│   └── 456-*.avro            # Manifest file (data file list)
└── data/
    ├── 00000-*.parquet       # Data file from INSERT
    ├── 00001-*.parquet       # Data file from INSERT
    └── 00002-*.parquet       # Data file from UPDATE
```

### Snapshots

Each operation (INSERT, DELETE, UPDATE) creates an immutable snapshot:
- Snapshots are atomic - all changes succeed or fail together
- Old snapshots preserved for time travel
- Garbage collection removes old snapshots when configured

### Copy-on-Write vs Merge-on-Read

| Mode | DELETE/UPDATE | SELECT | Use Case |
|------|---------------|--------|----------|
| Copy-on-Write | Rewrites files | Fast reads | Read-heavy workloads |
| Merge-on-Read | Creates delete files | Slower reads | Write-heavy workloads |

AIStor uses copy-on-write by default.

## Troubleshooting

### "Connection refused" error
Ensure services are running:
```bash
docker ps
./scripts/start_services.sh
```

### "License required" error
Add your MinIO license to `docker/.env`:
```bash
MINIO_LICENSE=your-license-key-here
```

### Trino catalog errors
The lab creates a dynamic catalog. If issues persist:
```bash
./scripts/stop_services.sh --clean
./scripts/start_services.sh
```
