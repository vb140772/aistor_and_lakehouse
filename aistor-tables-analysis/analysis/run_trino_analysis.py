#!/usr/bin/env python3
"""
Trino Iceberg Analysis Script
Load Parquet data into Iceberg table using AIStor catalog and run analysis queries
Compare performance with DuckDB results
"""

import os
import sys
import time
import glob
import json
import logging

# Configure logging early
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Force stdout to be unbuffered
sys.stdout.reconfigure(line_buffering=True)

logger.info("Starting script...")

import requests
logger.info("Imported requests")
import boto3
logger.info("Imported boto3")
import pandas as pd
logger.info("Imported pandas")
from pathlib import Path
from typing import List, Tuple, Optional, Dict
from trino.dbapi import connect
logger.info("Imported trino")

# Import sigv4 module for REST API authentication
# Try local module first, then fall back to container path
try:
    # Try importing from same directory (analysis package)
    from . import sigv4
except ImportError:
    try:
        # Try importing as standalone module (when run directly)
        import sigv4
    except ImportError:
        # sigv4 not available - warehouse REST API calls will be skipped
        sigv4 = None

# Configuration from environment variables
TRINO_URI = os.getenv("TRINO_URI", "http://localhost:9999")
MINIO_HOST = os.getenv("MINIO_HOST", "http://localhost:9000")
# For Trino catalog (running in Docker), use 'minio:9000' (Docker service name)
# For Python API calls and Spark, use MINIO_HOST (can be localhost:9000 from host)
MINIO_HOST_FOR_TRINO = os.getenv("MINIO_HOST_FOR_TRINO", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
WAREHOUSE = os.getenv("WAREHOUSE", "trinotutorial")
PARQUET_DIR = Path(os.getenv("PARQUET_DIR", "./data/parquet"))
EXECUTION_MODE = os.getenv("EXECUTION_MODE", "local").lower()

# Catalog configuration
# Trino uses MINIO_HOST_FOR_TRINO (Docker service name), Python/Spark use MINIO_HOST
CATALOG_URL_FOR_TRINO = f"{MINIO_HOST_FOR_TRINO}/_iceberg"
CATALOG_URL = f"{MINIO_HOST}/_iceberg"  # Keep for backward compatibility
ICEBERG_CATALOG_NAME = "tutorial_catalog"
HIVE_CATALOG_NAME = "hive"  # Use static 'hive' catalog if available, fallback to dynamic
SCHEMA_NAME = "taxi_analysis"
TABLE_NAME = "taxi_trips_iceberg"
STAGING_BUCKET = f"{WAREHOUSE}-staging"

# Analysis query (same as DuckDB script)
ANALYSIS_QUERY = """
SELECT 
    company, 
    count(*) as trip_count, 
    sum(fare) as total_fare, 
    sum(fare)/count(*) as avg_fare
FROM {table_name}
GROUP BY company
ORDER BY trip_count DESC
"""


def find_parquet_files(directory: Path) -> List[str]:
    """Find all Parquet files in directory."""
    pattern = str(directory / "*.parquet")
    files = glob.glob(pattern)
    return sorted(files)


def upload_parquet_to_minio(parquet_files: List[str], s3_prefix: str = "parquet/") -> str:
    """Upload Parquet files to MinIO S3 and return S3 path prefix."""
    print(f"\n{'='*60}")
    print("Uploading Parquet files to MinIO")
    print(f"{'='*60}")
    
    s3 = boto3.client('s3',
        endpoint_url=MINIO_HOST,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='dummy'
    )
    
    # Ensure staging bucket exists
    try:
        s3.head_bucket(Bucket=STAGING_BUCKET)
        print(f"✓ Staging bucket '{STAGING_BUCKET}' exists")
    except:
        print(f"Creating staging bucket '{STAGING_BUCKET}'...")
        s3.create_bucket(Bucket=STAGING_BUCKET)
        print(f"✓ Staging bucket created")
    
    # Upload files
    uploaded_count = 0
    for parquet_file in parquet_files:
        file_name = os.path.basename(parquet_file)
        s3_key = f"{s3_prefix}{file_name}"
        
        print(f"Uploading {file_name}...")
        s3.upload_file(parquet_file, STAGING_BUCKET, s3_key)
        uploaded_count += 1
    
    print(f"✓ Uploaded {uploaded_count} file(s) to s3://{STAGING_BUCKET}/{s3_prefix}")
    return f"s3://{STAGING_BUCKET}/{s3_prefix}"


def ensure_warehouse(s3_client):
    """Ensure warehouse exists in AIStor."""
    print(f"\n{'='*60}")
    print("Ensuring warehouse exists")
    print(f"{'='*60}")
    
    try:
        buckets = s3_client.list_buckets()
        bucket_names = [b['Name'] for b in buckets['Buckets']]
        
        if WAREHOUSE not in bucket_names:
            print(f"Creating bucket '{WAREHOUSE}'...")
            try:
                s3_client.create_bucket(Bucket=WAREHOUSE)
                print(f"✓ Bucket created")
            except s3_client.exceptions.BucketAlreadyOwnedByYou:
                print(f"✓ Bucket '{WAREHOUSE}' already exists")
            except Exception as e:
                # Check if it's a bucket already exists error
                if "BucketAlreadyOwnedByYou" in str(e) or "BucketAlreadyExists" in str(e):
                    print(f"✓ Bucket '{WAREHOUSE}' already exists")
                else:
                    raise
        else:
            print(f"✓ Bucket '{WAREHOUSE}' exists")
    except Exception as e:
        # Check if bucket already exists
        if "BucketAlreadyOwnedByYou" in str(e) or "BucketAlreadyExists" in str(e):
            print(f"✓ Bucket '{WAREHOUSE}' already exists")
        else:
            print(f"✗ Error checking bucket: {e}")
            raise
    
    # Check if warehouse exists in catalog
    if sigv4:
        try:
            warehouse_url = f"{CATALOG_URL}/v1/warehouses"
            headers_to_sign = {}
            aws_sign = sigv4.sign('GET', url=warehouse_url, body='', host=MINIO_HOST,
                               access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, 
                               headers=headers_to_sign)
            response = requests.get(warehouse_url, headers=aws_sign.headers, timeout=5)
            
            if response.status_code == 200:
                warehouses = response.json().get('warehouses', [])
                if WAREHOUSE in warehouses:
                    print(f"✓ Warehouse '{WAREHOUSE}' exists in catalog")
                    return
        except Exception as e:
            print(f"Warning: Could not check warehouse in catalog: {e}")
    
    # Create warehouse if needed (with upgrade-existing flag if bucket exists)
    if sigv4:
        try:
            warehouse_url = f"{CATALOG_URL}/v1/warehouses"
            # Use upgrade-existing flag to convert existing bucket to warehouse
            payload = json.dumps({"name": WAREHOUSE, "upgrade-existing": True})
            headers_to_sign = {
                "content-type": "application/json",
                "content-length": str(len(payload))
            }
            aws_sign = sigv4.sign('POST', url=warehouse_url, body=payload, host=MINIO_HOST,
                                 access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY,
                                 headers=headers_to_sign)
            response = requests.post(warehouse_url, data=payload, headers=aws_sign.headers)
            
            if response.status_code in [200, 201, 409]:
                print(f"✓ Warehouse '{WAREHOUSE}' ready")
            else:
                print(f"Warning: Warehouse creation returned {response.status_code}: {response.text[:200]}")
        except Exception as e:
            print(f"Warning: Could not create warehouse via API: {e}")


def run_spark_in_container(staging_bucket: str, schema: str, table: str) -> bool:
    """
    Run Spark ingestion inside the Spark Docker container.
    
    This eliminates the need for Java to be installed on the host machine.
    The Spark container has Java 17 bundled.
    """
    import subprocess
    
    logger.info("Entering run_spark_in_container")
    print(f"\n{'='*60}")
    print("Running Spark ingestion in container")
    print(f"{'='*60}")
    sys.stdout.flush()
    
    # Spark packages for Iceberg support (Spark 3.5 compatible)
    spark_packages = (
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
        "org.apache.iceberg:iceberg-aws-bundle:1.6.1,"
        "org.apache.hadoop:hadoop-aws:3.3.4"
    )
    
    # Build docker exec command
    cmd = [
        "docker", "exec", "docker-spark-1",
        "/opt/spark/bin/spark-submit",
        "--packages", spark_packages,
        "/app/analysis/spark_ingest.py",
        "--warehouse", WAREHOUSE,
        "--staging-bucket", staging_bucket,
        "--minio-host", "http://minio:9000",  # Docker network address
        "--access-key", MINIO_ACCESS_KEY,
        "--secret-key", MINIO_SECRET_KEY,
        "--schema", schema,
        "--table", table
    ]
    
    print(f"Executing: docker exec docker-spark-1 spark-submit ...")
    print("This may take a while for large datasets...")
    
    try:
        # Run spark-submit in container
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )
        
        # Print output
        if result.stdout:
            # Filter to show key lines
            for line in result.stdout.split('\n'):
                if any(x in line for x in ['Read', 'rows', 'Table', 'Error', 'loaded', 'created', '=']):
                    print(line)
        
        if result.returncode != 0:
            print(f"✗ Spark ingestion failed (exit code {result.returncode})")
            if result.stderr:
                # Show last 20 lines of stderr
                stderr_lines = result.stderr.strip().split('\n')
                for line in stderr_lines[-20:]:
                    print(f"  {line}")
            return False
        
        print("✓ Spark ingestion completed successfully")
        return True
        
    except subprocess.TimeoutExpired:
        print("✗ Spark ingestion timed out (>10 minutes)")
        return False
    except FileNotFoundError:
        print("✗ Docker not found. Make sure Docker is running.")
        return False
    except Exception as e:
        print(f"✗ Failed to run Spark in container: {e}")
        return False


def create_iceberg_catalog(cur) -> bool:
    """Create or verify AIStor Iceberg catalog in Trino."""
    print(f"\n{'='*60}")
    print("Setting up AIStor Iceberg catalog")
    print(f"{'='*60}")
    
    try:
        # Drop existing catalog if it exists (to ensure fresh config)
        try:
            cur.execute(f"DROP CATALOG IF EXISTS {ICEBERG_CATALOG_NAME}")
        except:
            pass
        
        # Create Iceberg catalog
        # Use MINIO_HOST_FOR_TRINO for Trino (Docker service name), not localhost
        cur.execute(f"""
            CREATE CATALOG {ICEBERG_CATALOG_NAME} USING iceberg
            WITH (
                "iceberg.catalog.type" = 'rest',
                "iceberg.rest-catalog.uri" = '{CATALOG_URL_FOR_TRINO}',
                "iceberg.rest-catalog.warehouse" = '{WAREHOUSE}',
                "iceberg.rest-catalog.security" = 'SIGV4',
                "iceberg.rest-catalog.vended-credentials-enabled" = 'true',
                "iceberg.unique-table-location" = 'true',
                "iceberg.rest-catalog.signing-name" = 's3tables',
                "iceberg.rest-catalog.view-endpoints-enabled" = 'true',
                "s3.region" = 'dummy',
                "s3.aws-access-key" = '{MINIO_ACCESS_KEY}',
                "s3.aws-secret-key" = '{MINIO_SECRET_KEY}',
                "s3.endpoint" = '{MINIO_HOST_FOR_TRINO}',
                "s3.path-style-access" = 'true',
                "fs.hadoop.enabled" = 'false',
                "fs.native-s3.enabled" = 'true'
            )
        """)
        print(f"✓ Iceberg catalog '{ICEBERG_CATALOG_NAME}' created")
        return True
    except Exception as e:
        print(f"✗ Failed to create Iceberg catalog: {e}")
        return False


def create_hive_catalog(cur, s3_path: str) -> bool:
    """Create Hive catalog for reading Parquet files from S3."""
    print(f"\n{'='*60}")
    print("Setting up Hive catalog for Parquet files")
    print(f"{'='*60}")
    
    try:
        # Drop existing catalog if it exists
        try:
            cur.execute(f"DROP CATALOG IF EXISTS {HIVE_CATALOG_NAME}")
        except:
            pass
        
        # Create Hive catalog pointing to MinIO S3
        # Note: Trino's Hive connector uses filesystem properties for S3 access
        # Using file-based metastore with a temporary directory
        # S3 filesystem configuration uses the same properties as Iceberg connector
        cur.execute(f"""
            CREATE CATALOG {HIVE_CATALOG_NAME} USING hive
            WITH (
                "hive.metastore" = 'file',
                "hive.metastore.catalog.dir" = 'file:///tmp/hive-metastore-{HIVE_CATALOG_NAME}',
                "fs.native-s3.enabled" = 'true',
                "fs.hadoop.enabled" = 'false',
                "s3.endpoint" = '{MINIO_HOST}',
                "s3.aws-access-key" = '{MINIO_ACCESS_KEY}',
                "s3.aws-secret-key" = '{MINIO_SECRET_KEY}',
                "s3.path-style-access" = 'true',
                "s3.region" = 'us-east-1'
            )
        """)
        print(f"✓ Hive catalog '{HIVE_CATALOG_NAME}' created")
        return True
    except Exception as e:
        print(f"✗ Failed to create Hive catalog: {e}")
        print(f"  Note: Hive catalog may not be available. Will try alternative approach.")
        return False


def create_hive_table_from_s3(cur, s3_path: str, table_name: str = "taxi_trips_parquet") -> bool:
    """Create Hive external table pointing to Parquet files in S3."""
    print(f"\n{'='*60}")
    print("Creating Hive external table")
    print(f"{'='*60}")
    
    try:
        # Connect to hive catalog using the same TRINO_URI
        conn_hive = connect(host=TRINO_URI, user="trino", catalog=HIVE_CATALOG_NAME, request_timeout=120)
        cur_hive = conn_hive.cursor()
        
        # Create schema first - use information_schema to check
        try:
            # Try creating schema
            cur_hive.execute("CREATE SCHEMA IF NOT EXISTS default")
            print("✓ Schema 'default' created/verified")
        except Exception as e:
            print(f"Note: Schema creation: {e}")
        
        # Drop table if exists
        try:
            cur_hive.execute(f"DROP TABLE IF EXISTS default.{table_name}")
        except:
            pass
        
        # Create external table with simplified schema first (just columns we need)
        # This avoids schema mismatch issues
        s3_dir_path = s3_path.rstrip('*').rstrip('/')
        
        # Start with minimal schema for the analysis query
        create_table_sql = f"""
            CREATE TABLE default.{table_name} (
                company VARCHAR,
                fare DOUBLE
            )
            WITH (
                format = 'PARQUET',
                external_location = '{s3_dir_path}/'
            )
        """
        
        cur_hive.execute(create_table_sql)
        print(f"✓ Hive table '{table_name}' created")
        
        # Verify table is accessible with a simple query
        try:
            cur_hive.execute(f"SELECT COUNT(*) FROM default.{table_name}")
            count = cur_hive.fetchone()[0]
            print(f"✓ Hive table is accessible, row count: {count:,}")
            return True
        except Exception as e:
            print(f"Warning: Could not verify table: {e}")
            # Still return True if table was created
            return True
            
    except Exception as e:
        print(f"✗ Failed to create Hive table: {e}")
        import traceback
        traceback.print_exc()
        return False




def load_data_via_pyarrow(iceberg_cur, parquet_files: List[str], iceberg_table: str) -> bool:
    """Load data into Iceberg table by reading Parquet files with PyArrow and inserting via Trino."""
    print(f"\n{'='*60}")
    print("Loading data using PyArrow + Trino INSERT")
    print(f"{'='*60}")
    
    try:
        import pyarrow.parquet as pq
        
        # Create schema in Iceberg catalog
        try:
            iceberg_cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
            print(f"✓ Schema '{SCHEMA_NAME}' created")
        except:
            pass
        
        # Drop table if exists
        try:
            iceberg_cur.execute(f"DROP TABLE IF EXISTS {ICEBERG_CATALOG_NAME}.{SCHEMA_NAME}.{iceberg_table}")
        except:
            pass
        
        # Create empty Iceberg table with just the columns we need for analysis
        print("Creating Iceberg table...")
        iceberg_cur.execute(f"""
            CREATE TABLE {ICEBERG_CATALOG_NAME}.{SCHEMA_NAME}.{iceberg_table} (
                company VARCHAR,
                fare DOUBLE
            )
        """)
        print("✓ Iceberg table created")
        
        # Read Parquet files and insert in batches
        print(f"Loading data from {len(parquet_files)} Parquet file(s)...")
        print("This may take a while...")
        
        total_rows = 0
        batch_size = 10000  # Insert in batches of 10k rows
        
        for i, parquet_file in enumerate(parquet_files):
            if (i + 1) % 50 == 0:
                print(f"  Processing file {i+1}/{len(parquet_files)}...")
            
            try:
                # Read Parquet file
                table = pq.read_table(parquet_file, columns=['company', 'fare'])
                df = table.to_pandas()
                
                # Insert in batches
                for start_idx in range(0, len(df), batch_size):
                    batch = df[start_idx:start_idx + batch_size]
                    
                    # Build INSERT statement with values
                    values = []
                    for _, row in batch.iterrows():
                        company = str(row['company']).replace("'", "''")  # Escape quotes
                        fare = row['fare'] if pd.notna(row['fare']) else 0
                        values.append(f"('{company}', {fare})")
                    
                    if values:
                        insert_sql = f"""
                            INSERT INTO {ICEBERG_CATALOG_NAME}.{SCHEMA_NAME}.{iceberg_table} (company, fare)
                            VALUES {','.join(values)}
                        """
                        iceberg_cur.execute(insert_sql)
                        total_rows += len(values)
            except Exception as e:
                print(f"Warning: Error processing {parquet_file}: {e}")
                continue
        
        print(f"✓ Loaded {total_rows:,} rows into Iceberg table")
        
        # Verify
        iceberg_cur.execute(f"SELECT COUNT(*) FROM {ICEBERG_CATALOG_NAME}.{SCHEMA_NAME}.{iceberg_table}")
        row_count = iceberg_cur.fetchone()[0]
        print(f"✓ Table contains {row_count:,} rows")
        
        return True
    except ImportError:
        print("✗ PyArrow not available - cannot use alternative loading method")
        return False
    except Exception as e:
        print(f"✗ Failed to load data: {e}")
        import traceback
        traceback.print_exc()
        return False


def create_iceberg_table_from_hive(iceberg_cur, hive_table: str, iceberg_table: str) -> bool:
    """Create Iceberg table and load data from Hive table using CTAS."""
    print(f"\n{'='*60}")
    print("Creating Iceberg table and loading data")
    print(f"{'='*60}")
    
    try:
        # Create schema in Iceberg catalog
        try:
            iceberg_cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
            print(f"✓ Schema '{SCHEMA_NAME}' created")
        except Exception as e:
            print(f"Schema may already exist: {e}")
        
        # Drop table if exists
        try:
            iceberg_cur.execute(f"DROP TABLE IF EXISTS {ICEBERG_CATALOG_NAME}.{SCHEMA_NAME}.{iceberg_table}")
        except:
            pass
        
        # Create Iceberg table using CTAS (Create Table As Select)
        # Since Hive table only has company and fare, create Iceberg table with same schema
        print(f"Loading data from Hive table to Iceberg table...")
        print(f"This may take a while for large datasets...")
        
        ctas_sql = f"""
            CREATE TABLE {ICEBERG_CATALOG_NAME}.{SCHEMA_NAME}.{iceberg_table} AS
            SELECT company, fare FROM {hive_table}
        """
        
        start_time = time.time()
        iceberg_cur.execute(ctas_sql)
        load_time = time.time() - start_time
        
        print(f"✓ Data loaded into Iceberg table in {load_time:.2f} seconds")
        
        # Verify row count
        iceberg_cur.execute(f"SELECT COUNT(*) FROM {ICEBERG_CATALOG_NAME}.{SCHEMA_NAME}.{iceberg_table}")
        row_count = iceberg_cur.fetchone()[0]
        print(f"✓ Table contains {row_count:,} rows")
        
        return True
    except Exception as e:
        print(f"✗ Failed to create Iceberg table: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_trino_query(iceberg_cur, table_name: str) -> Tuple[List, float]:
    """Run analysis query on Iceberg table and return results with execution time."""
    print(f"\n{'='*60}")
    print("Running analysis query on Iceberg table")
    print(f"{'='*60}")
    
    query = ANALYSIS_QUERY.format(table_name=table_name)
    print(f"Query: {query[:100]}...")
    
    start_time = time.time()
    iceberg_cur.execute(query)
    results = iceberg_cur.fetchall()
    execution_time = time.time() - start_time
    
    column_names = ['company', 'trip_count', 'total_fare', 'avg_fare']
    
    return results, execution_time, column_names


def print_results(results: List, column_names: List[str], format_name: str, execution_time: float):
    """Print query results in a formatted table."""
    print(f"\n{'='*60}")
    print(f"{format_name} Results (Execution time: {execution_time:.3f} seconds)")
    print(f"{'='*60}")
    
    if not results:
        print("No results returned")
        return
    
    # Print header
    header = " | ".join([f"{col:>15}" for col in column_names])
    print(header)
    print("-" * len(header))
    
    # Print rows (limit to top 20 for readability)
    for row in results[:20]:
        row_str = " | ".join([f"{str(val):>15}" for val in row])
        print(row_str)
    
    if len(results) > 20:
        print(f"... and {len(results) - 20} more rows")
    
    print(f"\nTotal rows: {len(results)}")


def compare_performance(trino_time: float, trino_results: List, 
                       duckdb_time: Optional[float] = None, 
                       duckdb_results: Optional[List] = None):
    """Compare and display performance metrics and results."""
    print(f"\n{'='*60}")
    print("Performance Summary")
    print(f"{'='*60}")
    print(f"Trino/Iceberg execution time: {trino_time:.3f} seconds")
    print(f"Trino/Iceberg result rows:     {len(trino_results)}")
    
    if duckdb_time and duckdb_results:
        print(f"DuckDB execution time:        {duckdb_time:.3f} seconds")
        print(f"DuckDB result rows:           {len(duckdb_results)}")
        
        # Compare row counts
        if len(trino_results) == len(duckdb_results):
            print(f"\n✓ Both queries returned {len(trino_results)} rows")
        else:
            print(f"\n⚠ Row count mismatch: Trino={len(trino_results)}, DuckDB={len(duckdb_results)}")
        
        # Compare performance
        if trino_time < duckdb_time:
            faster = "Trino/Iceberg"
            speedup = duckdb_time / trino_time
        else:
            faster = "DuckDB"
            speedup = trino_time / duckdb_time
        
        print(f"\n{faster} is {speedup:.2f}x faster")
        
        time_diff = abs(trino_time - duckdb_time)
        percent_diff = (time_diff / max(trino_time, duckdb_time)) * 100
        print(f"Time difference: {time_diff:.3f} seconds ({percent_diff:.1f}%)")
        
        # Compare top results (first 5 rows)
        print(f"\n{'='*60}")
        print("Result Comparison (Top 5 rows)")
        print(f"{'='*60}")
        print(f"{'Rank':<6} {'Company (Trino)':<30} {'Company (DuckDB)':<30} {'Match':<10}")
        print("-" * 76)
        
        for i in range(min(5, len(trino_results), len(duckdb_results))):
            trino_company = str(trino_results[i][0])[:28]
            duckdb_company = str(duckdb_results[i][0])[:28]
            match = "✓" if trino_company == duckdb_company else "✗"
            print(f"{i+1:<6} {trino_company:<30} {duckdb_company:<30} {match:<10}")


def run_duckdb_comparison(parquet_files: List[str]) -> Tuple[Optional[float], Optional[List]]:
    """Optionally run DuckDB analysis for comparison."""
    print(f"\n{'='*60}")
    print("Running DuckDB Analysis for Comparison")
    print(f"{'='*60}")
    
    try:
        import duckdb
    except ImportError:
        print("DuckDB not available - skipping comparison")
        return None, None
    
    try:
        conn = duckdb.connect()
        conn.execute("SET memory_limit='20GB'")
        conn.execute("SET temp_directory='./duckdb_temp'")
        conn.execute("SET threads=4")
        
        # Create view from Parquet files
        if len(parquet_files) == 1:
            table_expr = f"read_parquet('{parquet_files[0]}')"
        else:
            dir_path = os.path.dirname(parquet_files[0])
            table_expr = f"read_parquet('{dir_path}/*.parquet')"
        
        view_name = "taxi_data_parquet"
        conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {table_expr}")
        
        # Run query
        query = ANALYSIS_QUERY.format(table_name=view_name)
        start_time = time.time()
        result = conn.execute(query).fetchall()
        execution_time = time.time() - start_time
        
        conn.close()
        
        print(f"✓ DuckDB analysis completed in {execution_time:.3f} seconds")
        print(f"✓ DuckDB returned {len(result)} rows")
        
        return execution_time, result
    except Exception as e:
        print(f"✗ DuckDB analysis failed: {e}")
        return None, None


def main():
    """Main execution function."""
    logger.info("Entering main()")
    print("="*60)
    print("Trino Iceberg Analysis")
    print("="*60)
    sys.stdout.flush()
    print(f"Execution mode: {EXECUTION_MODE.upper()}")
    print(f"Parquet directory: {PARQUET_DIR}")
    print(f"Trino URI: {TRINO_URI}")
    print(f"MinIO Host: {MINIO_HOST}")
    print(f"Warehouse: {WAREHOUSE}")
    print("="*60)
    
    # Check if Parquet directory exists
    if not PARQUET_DIR.exists():
        print(f"Error: Parquet directory not found: {PARQUET_DIR}")
        if EXECUTION_MODE == "local":
            print("Please run: ./scripts/download_from_gcs_rsync.sh")
        return 1
    
    # Find Parquet files
    parquet_files = find_parquet_files(PARQUET_DIR)
    if not parquet_files:
        print(f"Error: No Parquet files found in {PARQUET_DIR}")
        return 1
    
    print(f"\nFound {len(parquet_files)} Parquet file(s)")
    
    # Setup S3 client
    s3_client = boto3.client('s3',
        endpoint_url=MINIO_HOST,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='dummy'
    )
    
    # Ensure warehouse exists BEFORE creating Trino catalog
    # This is critical - warehouse must be registered in catalog API first
    ensure_warehouse(s3_client)
    
    # Upload Parquet files to MinIO
    s3_path = upload_parquet_to_minio(parquet_files)
    
    # Connect to Trino
    print(f"\n{'='*60}")
    print("Connecting to Trino")
    print(f"{'='*60}")
    try:
        conn = connect(host=TRINO_URI, user="trino", request_timeout=300)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        print("✓ Connected to Trino")
    except Exception as e:
        print(f"✗ Failed to connect to Trino: {e}")
        return 1
    
    # Create catalogs (for Trino queries)
    # IMPORTANT: Drop and recreate catalog to ensure fresh configuration
    # This ensures the catalog is created AFTER warehouse is properly set up
    if not create_iceberg_catalog(cur):
        return 1
    
    # Connect to Iceberg catalog (for Trino queries)
    try:
        iceberg_conn = connect(host=TRINO_URI, user="trino", catalog=ICEBERG_CATALOG_NAME, request_timeout=300)
        iceberg_cur = iceberg_conn.cursor()
        print(f"✓ Connected to Iceberg catalog '{ICEBERG_CATALOG_NAME}'")
        
        # Ensure schema exists in Trino (Spark created it, but Trino needs to see it)
        try:
            iceberg_cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
            print(f"✓ Schema '{SCHEMA_NAME}' verified in Trino")
        except Exception as e:
            print(f"Note: Schema creation: {e}")
            # Try to list schemas to see what's available
            try:
                iceberg_cur.execute("SHOW SCHEMAS")
                schemas = [row[0] for row in iceberg_cur.fetchall()]
                print(f"Available schemas: {schemas}")
            except:
                pass
    except Exception as e:
        print(f"✗ Failed to connect to Iceberg catalog: {e}")
        return 1
    
    # Load data into Iceberg table using Spark container (with PyArrow fallback)
    # Spark runs in Docker container - no Java required on host
    if not run_spark_in_container(STAGING_BUCKET, SCHEMA_NAME, TABLE_NAME):
        print("\n⚠ Spark container ingestion failed, falling back to PyArrow method...")
        print("="*60)
        
        # Fallback to PyArrow method (slower but doesn't require Spark)
        if not load_data_via_pyarrow(iceberg_cur, parquet_files, TABLE_NAME):
            print("Failed to load data into Iceberg table")
            return 1
    
    # Run analysis query
    full_table_name = f"{ICEBERG_CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"
    trino_results, trino_time, trino_columns = run_trino_query(iceberg_cur, full_table_name)
    print_results(trino_results, trino_columns, "Trino/Iceberg", trino_time)
    
    # Optionally run DuckDB for comparison
    run_duckdb = os.getenv("COMPARE_WITH_DUCKDB", "false").lower() == "true"
    duckdb_time = None
    duckdb_results = None
    
    if run_duckdb:
        duckdb_time, duckdb_results = run_duckdb_comparison(parquet_files)
    
    # Performance comparison
    compare_performance(trino_time, trino_results, duckdb_time, duckdb_results)
    
    print(f"\n{'='*60}")
    print("Analysis complete!")
    print(f"{'='*60}")
    
    return 0


if __name__ == "__main__":
    exit(main())
