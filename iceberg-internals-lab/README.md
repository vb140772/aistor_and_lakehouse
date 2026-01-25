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
python lab/run_lab.py
```

The lab is interactive - press Enter to advance through each step.

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
