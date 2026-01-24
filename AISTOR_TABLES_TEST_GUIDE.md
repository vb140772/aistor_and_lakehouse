# AIStor Tables Integration Test Guide

## Overview

This document describes the `run_trino_analysis.py` test script, which demonstrates end-to-end integration of **MinIO AIStor Tables** (native Apache Iceberg REST Catalog) with **Apache Spark** for data ingestion and **Trino** for distributed SQL analytics.

### Purpose

The test validates:
1. Data ingestion into Iceberg tables via Spark
2. SQL querying via Trino with AIStor's native Iceberg REST Catalog
3. Performance comparison between Trino/Iceberg and DuckDB

### Key Technologies

| Component | Role | Runs On |
|-----------|------|---------|
| MinIO AIStor | Object storage + Iceberg REST Catalog | Docker container |
| Apache Spark (PySpark) | Data ingestion (Parquet → Iceberg) | Host machine (local mode) |
| Trino | SQL query engine | Docker container |
| Apache Iceberg | Table format | - |
| DuckDB | Local SQL engine (comparison) | Host machine |
| Python script | Orchestration (`run_trino_analysis.py`) | Host machine |

---

## Architecture

### System Overview

```mermaid
flowchart TB
    subgraph Local["Host Machine (Python Script)"]
        Script["run_trino_analysis.py"]
        Files["Local Parquet Files<br/>424 files, 4.2GB"]
        Spark["PySpark<br/>(local mode)"]
        DuckDB["DuckDB<br/>(Optional)"]
        Results["Results<br/>Comparison"]
        
        Script --> Files
        Script --> Spark
        Script --> DuckDB
    end

    subgraph Docker["Docker Network: iceberg_tests"]
        subgraph MinIO["MinIO AIStor Container"]
            Storage["Object Storage<br/>S3 API: 9000"]
            Catalog["Iceberg REST Catalog<br/>/_iceberg/v1/..."]
        end
        
        Trino["Trino Container<br/>(Single Node)"]
    end

    %% Data Flow
    Files -->|"1. Upload (boto3)"| Storage
    Spark -->|"2. Read (s3a://)"| Storage
    Spark -->|"3. Write Iceberg"| Storage
    Spark -.->|"Create table"| Catalog
    
    Trino -->|"4. REST API<br/>SigV4 Auth"| Catalog
    Trino -->|"Read data"| Storage
    
    Script -->|"5. SQL Query<br/>(trino client)"| Trino
    Trino -->|"Results"| Results
    
    Files -->|"Direct read"| DuckDB
    DuckDB -->|"Compare"| Results

    %% Styling
    style MinIO fill:#ff6b6b,color:#fff
    style Catalog fill:#4ecdc4,color:#fff
    style Trino fill:#45b7d1,color:#fff
    style Spark fill:#f9ca24,color:#000
    style DuckDB fill:#6c5ce7,color:#fff
    style Script fill:#e17055,color:#fff
```

### Data Flow

```mermaid
sequenceDiagram
    participant L as Local Files
    participant S3 as MinIO S3
    participant Cat as Iceberg Catalog
    participant Spark as Apache Spark
    participant Trino as Trino
    participant Duck as DuckDB

    Note over L,Duck: Phase 1: Data Upload
    L->>S3: Upload Parquet files (boto3)
    S3-->>L: s3://staging-bucket/parquet/

    Note over L,Duck: Phase 2: Warehouse Setup
    S3->>Cat: Create/verify warehouse
    Cat-->>S3: Warehouse ready

    Note over L,Duck: Phase 3: Spark Data Ingestion
    Spark->>S3: Read Parquet (s3a://)
    Spark->>Cat: Create Iceberg table
    Spark->>S3: Write Iceberg data files
    Cat-->>Spark: Table metadata updated

    Note over L,Duck: Phase 4: Trino Query
    Trino->>Cat: Connect via REST (SigV4)
    Trino->>S3: Read Iceberg data
    Trino-->>Trino: Execute SQL

    Note over L,Duck: Phase 5: Comparison (Optional)
    Duck->>L: Read local Parquet
    Duck-->>Duck: Execute SQL
    Trino-->>Duck: Compare results
```

---

## Deployment

### Docker Compose Stack

This repository includes a self-contained Docker Compose configuration for MinIO AIStor + Trino.

**File**: `docker/docker-compose.yaml`

```yaml
services:
  minio:
    image: ${MINIO_TEST_IMAGE:-quay.io/minio/aistor/minio:EDGE}
    hostname: minio
    command: server --console-address ":9001" /data
    ports:
      - "9000:9000"   # S3 API
      - "9001:9001"   # Console
    environment:
      MINIO_LICENSE: ${MINIO_LICENSE}
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 5s
      timeout: 10s
      retries: 5
      start_period: 10s

  trino:
    image: ${TRINO_IMAGE:-trinodb/trino:477}
    hostname: trino
    environment:
      - CATALOG_MANAGEMENT=dynamic
    depends_on:
      minio:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-I", "http://localhost:8080/v1/status"]
      interval: 2s
      timeout: 10s
      retries: 5
      start_period: 10s
    ports:
      - "9999:8080"   # Trino REST API

networks:
  lakehouse:

volumes:
  minio_data:
```

### Start Commands

**Using helper script (recommended)**:
```bash
./scripts/start_services.sh
```

**Using Docker Compose directly**:
```bash
cd docker
docker compose up -d
```

### Stop Commands

**Using helper script**:
```bash
./scripts/stop_services.sh        # Preserve data
./scripts/stop_services.sh --clean  # Remove all data
```

**Using Docker Compose directly**:
```bash
cd docker
docker compose down       # Preserve data
docker compose down -v    # Remove all data
```

### Network Architecture

```mermaid
flowchart LR
    subgraph Host["Host Machine"]
        Script["Python Script<br/>run_trino_analysis.py"]
    end

    subgraph Docker["Docker Network: iceberg_tests"]
        subgraph MinIOContainer["minio container"]
            MinIO["MinIO AIStor<br/>S3: 9000<br/>Console: 9001"]
        end
        
        subgraph TrinoContainer["trino container"]
            Trino["Trino Server<br/>Port: 8080"]
            Note["Uses: minio:9000<br/>NOT localhost:9000"]
        end
    end

    Script -->|"localhost:9000<br/>(S3 API)"| MinIO
    Script -->|"localhost:9999<br/>(Trino API)"| Trino
    Trino -->|"minio:9000<br/>(internal Docker DNS)"| MinIO

    style MinIO fill:#ff6b6b,color:#fff
    style Trino fill:#45b7d1,color:#fff
    style Script fill:#f9ca24,color:#000
```

> **CRITICAL**: The script uses TWO different MinIO hostnames:
> - `MINIO_HOST = "http://localhost:9000"` — for Python/Spark on host
> - `MINIO_HOST_FOR_TRINO = "http://minio:9000"` — for Trino in Docker

---

## Spark Configuration

### Complete Spark Session Setup

Spark is configured to connect to AIStor's Iceberg REST Catalog with SigV4 authentication:

```python
spark = (
    SparkSession.builder
    .appName("AIStor Tables - Trino Analysis")
    
    # JAR Dependencies
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,"
            "org.apache.iceberg:iceberg-aws-bundle:1.10.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4")
    
    # Iceberg SQL Extensions
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    
    # Catalog Configuration
    .config("spark.sql.catalog.tutorial_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.tutorial_catalog.type", "rest")
    .config("spark.sql.catalog.tutorial_catalog.uri", "http://localhost:9000/_iceberg")
    .config("spark.sql.catalog.tutorial_catalog.warehouse", "trinotutorial")
    
    # REST Endpoint Credentials (for SigV4)
    .config("spark.sql.catalog.tutorial_catalog.rest.endpoint", "http://localhost:9000")
    .config("spark.sql.catalog.tutorial_catalog.rest.access-key-id", "minioadmin")
    .config("spark.sql.catalog.tutorial_catalog.rest.secret-access-key", "minioadmin")
    
    # SigV4 Authentication
    .config("spark.sql.catalog.tutorial_catalog.rest.sigv4-enabled", "true")
    .config("spark.sql.catalog.tutorial_catalog.rest.signing-name", "s3tables")
    .config("spark.sql.catalog.tutorial_catalog.rest.signing-region", "us-east-1")
    
    # S3 Data Access
    .config("spark.sql.catalog.tutorial_catalog.s3.access-key-id", "minioadmin")
    .config("spark.sql.catalog.tutorial_catalog.s3.secret-access-key", "minioadmin")
    .config("spark.sql.catalog.tutorial_catalog.s3.endpoint", "http://localhost:9000")
    .config("spark.sql.catalog.tutorial_catalog.s3.path-style-access", "true")
    
    # S3A Filesystem (for reading Parquet from S3)
    .config("spark.hadoop.fs.s3a.endpoint", "localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    # S3A Timeouts (MUST be numeric, not duration strings!)
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
    
    .getOrCreate()
)
```

### Key Configuration Groups

| Group | Purpose | Critical Settings |
|-------|---------|-------------------|
| **JAR Dependencies** | Load Iceberg + S3 support | `iceberg-spark-runtime`, `iceberg-aws-bundle`, `hadoop-aws` |
| **Catalog** | Connect to AIStor | `type=rest`, `uri`, `warehouse` |
| **REST Auth** | SigV4 for catalog API | `sigv4-enabled`, `signing-name`, credentials |
| **S3 Access** | Read/write data files | `s3.endpoint`, `s3.path-style-access`, credentials |
| **S3A Hadoop** | Read Parquet from staging | `fs.s3a.endpoint`, `fs.s3a.path.style.access` |

### Java Version Compatibility

**CRITICAL**: Spark requires Java 8-21. Java 24+ causes:
```
java.lang.UnsupportedOperationException: getSubject is not supported
```

**Fix**:
```bash
# Install Java 21
brew install openjdk@21

# Set JAVA_HOME
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
export PATH="$JAVA_HOME/bin:$PATH"
```

---

## Trino Configuration

### Dynamic Catalog Creation

Trino connects to AIStor using a dynamically created Iceberg catalog:

```sql
CREATE CATALOG tutorial_catalog USING iceberg
WITH (
    -- Catalog Type
    "iceberg.catalog.type" = 'rest',
    "iceberg.rest-catalog.uri" = 'http://minio:9000/_iceberg',
    "iceberg.rest-catalog.warehouse" = 'trinotutorial',
    
    -- Security (SigV4)
    "iceberg.rest-catalog.security" = 'SIGV4',
    "iceberg.rest-catalog.signing-name" = 's3tables',
    "iceberg.rest-catalog.vended-credentials-enabled" = 'true',
    "iceberg.rest-catalog.view-endpoints-enabled" = 'true',
    
    -- Table Location
    "iceberg.unique-table-location" = 'true',
    
    -- S3 Configuration
    "s3.region" = 'dummy',
    "s3.aws-access-key" = 'minioadmin',
    "s3.aws-secret-key" = 'minioadmin',
    "s3.endpoint" = 'http://minio:9000',
    "s3.path-style-access" = 'true',
    
    -- Filesystem
    "fs.hadoop.enabled" = 'false',
    "fs.native-s3.enabled" = 'true'
);
```

### Key Properties Explained

| Property | Value | Purpose |
|----------|-------|---------|
| `iceberg.catalog.type` | `rest` | Use Iceberg REST Catalog protocol |
| `iceberg.rest-catalog.uri` | `http://minio:9000/_iceberg` | AIStor catalog endpoint |
| `iceberg.rest-catalog.security` | `SIGV4` | AWS-style authentication |
| `iceberg.rest-catalog.signing-name` | `s3tables` | AIStor-specific service name |
| `iceberg.rest-catalog.vended-credentials-enabled` | `true` | Get S3 creds from catalog |
| `s3.path-style-access` | `true` | Required for MinIO |
| `fs.native-s3.enabled` | `true` | Use Trino's native S3 (faster) |

### Trino Cluster Configuration (Not Tested)

> **Warning**: Multi-node Trino cluster was not successfully tested with this analysis script.
> Use the single-node deployment (`docker/docker-compose.yaml`) for verified functionality.

For reference, multi-node Trino requires these configurations:

**Coordinator config.properties**:
```properties
coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=8080
discovery.uri=http://trino-coordinator:8080
```

**Worker config.properties**:
```properties
coordinator=false
http-server.http.port=8080
discovery.uri=http://trino-coordinator:8080
```

---

## Test Execution Flow

### Step-by-Step Process

```mermaid
flowchart TD
    subgraph Phase1["1. INITIALIZATION"]
        I1[Load environment variables]
        I2[Validate PARQUET_DIR exists]
        I3[Find Parquet files<br/>424 files, 4.2GB]
        I1 --> I2 --> I3
    end

    subgraph Phase2["2. UPLOAD TO MINIO"]
        U1[Create staging bucket]
        U2[Upload Parquet files]
        U3[Return S3 path prefix]
        U1 --> U2 --> U3
    end

    subgraph Phase3["3. WAREHOUSE SETUP"]
        W1[Create bucket if needed]
        W2[Check warehouse via REST API]
        W3[Create warehouse<br/>upgrade-existing=true]
        W1 --> W2 --> W3
    end

    subgraph Phase4["4. SPARK DATA INGESTION"]
        S1[Create Spark session]
        S2[Create namespace: taxi_analysis]
        S3[Read Parquet from s3a://]
        S4[Write to Iceberg table]
        S5[Verify row count]
        S1 --> S2 --> S3 --> S4 --> S5
    end

    subgraph Phase5["5. TRINO CATALOG SETUP"]
        T1[Connect to Trino]
        T2[DROP CATALOG IF EXISTS]
        T3[CREATE CATALOG]
        T4[Verify schema visibility]
        T1 --> T2 --> T3 --> T4
    end

    subgraph Phase6["6. QUERY EXECUTION"]
        Q1[Run analysis query]
        Q2[Measure execution time]
        Q3[Collect results]
        Q1 --> Q2 --> Q3
    end

    subgraph Phase7["7. COMPARISON (Optional)"]
        C1[Run DuckDB query]
        C2[Compare times]
        C3[Verify consistency]
        C1 --> C2 --> C3
    end

    subgraph Phase8["8. RESULTS"]
        R1[Print results table]
        R2[Display metrics]
        R3[Show summary]
        R1 --> R2 --> R3
    end

    Phase1 --> Phase2 --> Phase3 --> Phase4 --> Phase5 --> Phase6 --> Phase7 --> Phase8

    style Phase1 fill:#3498db,color:#fff
    style Phase2 fill:#e74c3c,color:#fff
    style Phase3 fill:#9b59b6,color:#fff
    style Phase4 fill:#f39c12,color:#fff
    style Phase5 fill:#1abc9c,color:#fff
    style Phase6 fill:#2ecc71,color:#fff
    style Phase7 fill:#95a5a6,color:#fff
    style Phase8 fill:#27ae60,color:#fff
```

### Analysis Query

The test runs this aggregation query on both engines:

```sql
SELECT 
    company, 
    count(*) as trip_count, 
    sum(fare) as total_fare, 
    sum(fare)/count(*) as avg_fare
FROM tutorial_catalog.taxi_analysis.taxi_trips_iceberg
GROUP BY company
ORDER BY trip_count DESC
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TRINO_URI` | `http://localhost:8080` | Trino REST API endpoint |
| `MINIO_HOST` | `http://localhost:9000` | MinIO S3 endpoint (for Python/Spark) |
| `MINIO_HOST_FOR_TRINO` | `http://minio:9000` | MinIO endpoint for Trino (Docker) |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `WAREHOUSE` | `trinotutorial` | Iceberg warehouse name |
| `PARQUET_DIR` | `./data/parquet` | Local Parquet files directory |
| `COMPARE_WITH_DUCKDB` | `false` | Enable DuckDB comparison |
| `EXECUTION_MODE` | `local` | Execution mode (local/docker) |

---

## Troubleshooting

### Common Issues

#### 1. "Cannot obtain metadata" Error

**Symptom**:
```
TrinoExternalError: ICEBERG_CATALOG_ERROR: Cannot obtain metadata
```

**Cause**: Trino is trying to reach MinIO using `localhost:9000` instead of `minio:9000`.

**Fix**: Ensure `MINIO_HOST_FOR_TRINO` is set to `http://minio:9000` in the Trino catalog creation.

#### 2. Spark Java Compatibility

**Symptom**:
```
java.lang.UnsupportedOperationException: getSubject is not supported
```

**Cause**: Java 24+ is incompatible with Spark/Hadoop.

**Fix**: Use Java 8-21:
```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
```

#### 3. S3A NumberFormatException

**Symptom**:
```
java.lang.NumberFormatException: For input string: "60s"
```

**Cause**: S3A properties expect numeric values, not duration strings.

**Fix**: Use milliseconds:
```python
.config("spark.hadoop.fs.s3a.connection.timeout", "60000")  # Not "60s"
```

#### 4. Warehouse Already Exists

**Symptom**:
```
warehouse name 'trinotutorial' is invalid
```

**Fix**: The script uses `upgrade-existing=True` to convert existing buckets to warehouses.

### Validation Commands

```bash
# Check MinIO health
curl http://localhost:9000/minio/health/live

# Check Trino health
curl http://localhost:9999/v1/status

# List warehouses via Iceberg REST API
curl -X GET http://localhost:9000/_iceberg/v1/warehouses \
  -H "Authorization: AWS4-HMAC-SHA256 ..."

# Trino CLI query
docker exec -it trino trino --execute "SHOW CATALOGS"
```

---

## Performance Results

### Test Environment

- **Dataset**: ~58 million taxi trips
- **Files**: 424 Parquet files (4.2GB total)
- **Query**: GROUP BY company with aggregations
- **Trino Configuration**: Single-node deployment (`docker/docker-compose.yaml`)

> **Note**: These results are from the **single-node Trino** deployment.
> Multi-node cluster was not successfully tested.

### Results

| Engine | Execution Time | Notes |
|--------|----------------|-------|
| **DuckDB** | 0.193s | Local, in-process |
| **Trino/Iceberg** | 0.465s | Single-node Docker, via REST catalog |

### Breakdown

| Metric | DuckDB | Trino/Iceberg |
|--------|--------|---------------|
| **Query Execution** | 0.193s | 0.465s |
| **Data Loading** | N/A (direct read) | 6.78s (Spark) |
| **Total Time** | 0.193s | ~7.25s |

**Observations**:
- DuckDB is **2.41x faster** for single-node local queries
- Trino includes network overhead (Docker containers) and distributed coordinator logic
- Trino advantage would emerge with distributed cluster and larger datasets
- Iceberg provides ACID, time travel, schema evolution (not benchmarked here)

### Expected Cluster Improvements (Theoretical)

If the multi-node cluster were working:
- **3 workers**: ~30-40% improvement estimated (0.27-0.32s)
- **5 workers**: ~40-50% improvement estimated (0.23-0.28s)

*These are theoretical estimates, not actual measurements.*

---

## Files Reference

| File | Purpose |
|------|---------|
| `analysis/run_trino_analysis.py` | Main test script |
| `analysis/sigv4.py` | SigV4 authentication for REST API |
| `scripts/run_trino_analysis.sh` | Shell wrapper with env vars |
| `scripts/start_services.sh` | Start MinIO + Trino services |
| `scripts/stop_services.sh` | Stop services |
| `docker/docker-compose.yaml` | MinIO + Trino deployment |
| `docker/.env` | Environment configuration |
| `docker/.env.example` | Environment template |

---

## Conclusion

This test demonstrates a complete data lakehouse architecture using:

1. **MinIO AIStor** as unified object storage with native Iceberg catalog
2. **Apache Spark** for scalable data ingestion
3. **Trino** (single-node) for interactive SQL analytics
4. **Apache Iceberg** as the open table format

### What Was Successfully Tested

- Spark loading Parquet data into Iceberg tables via AIStor REST catalog
- Trino (single-node) querying Iceberg tables with SigV4 authentication
- DuckDB vs Trino performance comparison on the same dataset
- End-to-end data flow from local files → MinIO → Iceberg → SQL results

### What Was NOT Tested

- Multi-node Trino cluster (configuration created but had startup issues)
- Cluster performance improvements
- Large-scale concurrent query workloads

The architecture enables:
- **Decoupled storage and compute**
- **Multiple query engines** on the same data
- **ACID transactions** on object storage
- **Schema evolution** without data rewriting
- **Time travel** for data versioning

### Future Work

For production deployments:
- Debug and test Trino cluster configuration
- Benchmark cluster performance with 3+ workers
- Add Spark cluster for large-scale ETL
- Tune resource allocation based on workload
