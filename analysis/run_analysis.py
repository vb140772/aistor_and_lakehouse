#!/usr/bin/env python3
"""
DuckDB Analysis Script
Run analytical queries against Avro and Parquet formats and compare performance
"""

import duckdb
import time
import os
import glob
from pathlib import Path
from typing import Dict, List, Tuple

# Directory paths - support both local and GKE execution modes
# Mode detection: environment variable EXECUTION_MODE or auto-detect based on path existence
EXECUTION_MODE = os.getenv("EXECUTION_MODE", "").lower()
GCS_MOUNT_PATH = os.getenv("GCS_MOUNT_PATH", "/mnt/gcs")

# Auto-detect mode if not explicitly set
if not EXECUTION_MODE:
    local_data_dir = Path("./data")
    gcs_mount_dir = Path(GCS_MOUNT_PATH)
    if local_data_dir.exists() and (local_data_dir / "avro").exists():
        EXECUTION_MODE = "local"
    elif gcs_mount_dir.exists():
        EXECUTION_MODE = "gke"
    else:
        # Default to local if neither exists (will fail gracefully with clear error)
        EXECUTION_MODE = "local"

# Set paths based on execution mode
if EXECUTION_MODE == "local":
    AVRO_DIR = Path("./data/avro")
    PARQUET_DIR = Path("./data/parquet")
else:  # GKE mode
    AVRO_DIR = Path(GCS_MOUNT_PATH) / "exports" / "dec_2023" / "avro"
    PARQUET_DIR = Path(GCS_MOUNT_PATH) / "exports" / "dec_2023" / "parquet"

# SQL Query to execute
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


def find_data_files(directory: Path, extension: str) -> List[str]:
    """Find all files with given extension in directory."""
    pattern = str(directory / f"*.{extension}")
    files = glob.glob(pattern)
    return sorted(files)


def run_query_on_format(conn: duckdb.DuckDBPyConnection, files: List[str], 
                        format_type: str, format_name: str) -> Tuple[List, float]:
    """
    Run the analysis query on files of a specific format.
    
    Returns:
        Tuple of (results, execution_time_seconds, column_names)
    """
    if not files:
        raise ValueError(f"No {format_name} files found in {files[0] if files else 'expected directory'}")
    
    print(f"\n{'='*60}")
    print(f"Analyzing {format_name} format")
    print(f"{'='*60}")
    print(f"Found {len(files)} {format_name} file(s)")
    
    # Create a view/table from the files
    # DuckDB can read multiple files if they have the same schema
    if format_type == "avro":
        if len(files) == 1:
            table_expr = f"read_avro('{files[0]}')"
        else:
            # For multiple files, use UNION ALL
            avro_reads = " UNION ALL ".join([f"SELECT * FROM read_avro('{f}')" for f in files])
            # Create view directly with UNION ALL
            view_name = f"taxi_data_{format_type}"
            conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS {avro_reads}")
            query = ANALYSIS_QUERY.format(table_name=view_name)
            print(f"\nExecuting query...")
            print(f"Query: {query[:100]}...")
            start_time = time.time()
            result = conn.execute(query).fetchall()
            execution_time = time.time() - start_time
            column_names = ['company', 'trip_count', 'total_fare', 'avg_fare']
            return result, execution_time, column_names
    elif format_type == "parquet":
        if len(files) == 1:
            table_expr = f"read_parquet('{files[0]}')"
        else:
            # DuckDB can read multiple parquet files directly using glob pattern
            # This is more memory efficient than passing a large list
            # Use the directory path with glob pattern
            dir_path = os.path.dirname(files[0])
            table_expr = f"read_parquet('{dir_path}/*.parquet')"
    else:
        raise ValueError(f"Unsupported format: {format_type}")
    
    # Register as a temporary view
    view_name = f"taxi_data_{format_type}"
    conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {table_expr}")
    
    # Execute the query with timing
    query = ANALYSIS_QUERY.format(table_name=view_name)
    
    print(f"\nExecuting query...")
    print(f"Query: {query[:100]}...")
    
    start_time = time.time()
    result = conn.execute(query).fetchall()
    execution_time = time.time() - start_time
    
    # Column names from our query (company, trip_count, total_fare, avg_fare)
    column_names = ['company', 'trip_count', 'total_fare', 'avg_fare']
    
    return result, execution_time, column_names


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


def compare_performance(avro_time: float, parquet_time: float):
    """Compare and display performance metrics."""
    print(f"\n{'='*60}")
    print("Performance Comparison")
    print(f"{'='*60}")
    print(f"Avro execution time:   {avro_time:.3f} seconds")
    print(f"Parquet execution time: {parquet_time:.3f} seconds")
    
    if avro_time < parquet_time:
        faster = "Avro"
        speedup = parquet_time / avro_time
    else:
        faster = "Parquet"
        speedup = avro_time / parquet_time
    
    print(f"\n{faster} is {speedup:.2f}x faster")
    
    time_diff = abs(avro_time - parquet_time)
    percent_diff = (time_diff / max(avro_time, parquet_time)) * 100
    print(f"Time difference: {time_diff:.3f} seconds ({percent_diff:.1f}%)")


def main():
    """Main execution function."""
    print("="*60)
    print("DuckDB Format Performance Analysis")
    print("="*60)
    print(f"Execution mode: {EXECUTION_MODE.upper()}")
    print(f"Avro directory: {AVRO_DIR}")
    print(f"Parquet directory: {PARQUET_DIR}")
    print("="*60)
    
    # Check if data directories exist
    if not AVRO_DIR.exists():
        print(f"Error: Avro directory not found: {AVRO_DIR}")
        if EXECUTION_MODE == "local":
            print("Please run: ./scripts/download_from_gcs_rsync.sh")
        else:
            print("Please ensure GCS FUSE is mounted and data is exported")
        return 1
    
    if not PARQUET_DIR.exists():
        print(f"Error: Parquet directory not found: {PARQUET_DIR}")
        if EXECUTION_MODE == "local":
            print("Please run: ./scripts/download_from_gcs_rsync.sh")
        else:
            print("Please ensure GCS FUSE is mounted and data is exported")
        return 1
    
    # Find data files
    avro_files = find_data_files(AVRO_DIR, "avro")
    parquet_files = find_data_files(PARQUET_DIR, "parquet")
    
    if not avro_files:
        print(f"Error: No Avro files found in {AVRO_DIR}")
        return 1
    
    if not parquet_files:
        print(f"Error: No Parquet files found in {PARQUET_DIR}")
        return 1
    
    # Create DuckDB connection with memory limit
    conn = duckdb.connect()
    
    # Set memory limit to 20GB and other memory-related settings
    # DuckDB accepts memory limit in bytes or as a string like '20GB'
    print(f"\nSetting DuckDB memory limit to 20GB...")
    conn.execute("SET memory_limit='20GB'")
    # Set temp directory to use disk for overflow
    conn.execute("SET temp_directory='./duckdb_temp'")
    # Limit threads to reduce memory pressure (fewer threads = less memory per thread)
    conn.execute("SET threads=4")
    
    # Install and load Avro extension if needed
    try:
        conn.execute("INSTALL avro")
        conn.execute("LOAD avro")
    except:
        pass  # Extension might already be installed or not needed
    
    try:
        # Run analysis on Avro files
        try:
            avro_results, avro_time, avro_columns = run_query_on_format(
                conn, avro_files, "avro", "Avro"
            )
            print_results(avro_results, avro_columns, "Avro", avro_time)
        except Exception as e:
            print(f"Error processing Avro files: {e}")
            avro_time = None
        
        # Run analysis on Parquet files
        try:
            parquet_results, parquet_time, parquet_columns = run_query_on_format(
                conn, parquet_files, "parquet", "Parquet"
            )
            print_results(parquet_results, parquet_columns, "Parquet", parquet_time)
        except Exception as e:
            print(f"Error processing Parquet files: {e}")
            parquet_time = None
        
        # Compare performance if both succeeded
        if avro_time is not None and parquet_time is not None:
            compare_performance(avro_time, parquet_time)
            
            # Verify results match
            print(f"\n{'='*60}")
            print("Result Verification")
            print(f"{'='*60}")
            
            # Compare row counts
            if len(avro_results) == len(parquet_results):
                print(f"✓ Both formats returned {len(avro_results)} rows")
            else:
                print(f"⚠ Row count mismatch: Avro={len(avro_results)}, Parquet={len(parquet_results)}")
        
    finally:
        conn.close()
    
    return 0


if __name__ == "__main__":
    exit(main())

