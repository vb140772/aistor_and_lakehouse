#!/usr/bin/env python3
"""
Download Chicago Taxi Data from BigQuery

Autonomously creates a subset of public Chicago taxi data, exports to GCS,
and downloads to local machine.

Usage:
    ./scripts/download_from_bq.py --project my-gcp-project
    ./scripts/download_from_bq.py --project my-project --years 3 --bucket my-bucket
"""

import argparse
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

try:
    from google.cloud import bigquery
    from google.cloud import storage
    from google.cloud.exceptions import NotFound, Conflict
except ImportError:
    print("Error: Google Cloud libraries not installed.")
    print("Run: pip install google-cloud-bigquery google-cloud-storage")
    sys.exit(1)


# Public dataset
PUBLIC_DATASET = "bigquery-public-data.chicago_taxi_trips.taxi_trips"

# Default settings
DEFAULT_YEARS = 5
DEFAULT_REGION = "US"  # Must match public dataset location


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Download Chicago taxi data from BigQuery",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --project my-gcp-project              # 5 years, auto bucket
  %(prog)s --project my-project --years 3        # 3 years of data
  %(prog)s --project my-project --bucket my-bkt  # Specific bucket
        """,
    )
    parser.add_argument(
        "--project", "-p",
        type=str,
        required=True,
        help="GCP project ID (required)",
    )
    parser.add_argument(
        "--years", "-y",
        type=int,
        default=DEFAULT_YEARS,
        help=f"Number of years of data (default: {DEFAULT_YEARS})",
    )
    parser.add_argument(
        "--bucket", "-b",
        type=str,
        default=None,
        help="GCS bucket name (default: auto-generated)",
    )
    parser.add_argument(
        "--region", "-r",
        type=str,
        default=DEFAULT_REGION,
        help=f"GCS/BQ region (default: {DEFAULT_REGION})",
    )
    parser.add_argument(
        "--output-dir", "-o",
        type=str,
        default=None,
        help="Output directory (default: taxi_data root)",
    )
    return parser.parse_args()


def create_dataset(client: bigquery.Client, project: str, region: str) -> str:
    """Create a temporary BigQuery dataset for export."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dataset_id = f"{project}.taxi_export_{timestamp}"
    
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = region
    
    print(f"Creating temporary dataset: {dataset_id}")
    client.create_dataset(dataset, exists_ok=True)
    
    return dataset_id


def get_max_year(client: bigquery.Client) -> int:
    """Get the most recent year available in the public dataset."""
    query = f"""
    SELECT MAX(EXTRACT(YEAR FROM trip_start_timestamp)) as max_year
    FROM `{PUBLIC_DATASET}`
    """
    result = client.query(query).result()
    max_year = list(result)[0].max_year
    return int(max_year)


def create_subset_table(
    client: bigquery.Client,
    dataset_id: str,
    years: int,
) -> tuple[str, int]:
    """Create a subset table from public Chicago taxi data."""
    table_id = f"{dataset_id}.taxi_trips"
    
    # Get the most recent year in the dataset (public data may be outdated)
    max_year = get_max_year(client)
    start_year = max_year - years + 1
    
    print(f"Dataset has data up to {max_year}")
    
    query = f"""
    CREATE OR REPLACE TABLE `{table_id}` AS
    SELECT
        unique_key,
        taxi_id,
        trip_start_timestamp,
        trip_end_timestamp,
        trip_seconds,
        trip_miles,
        pickup_census_tract,
        dropoff_census_tract,
        pickup_community_area,
        dropoff_community_area,
        fare,
        tips,
        tolls,
        extras,
        trip_total,
        payment_type,
        company,
        pickup_latitude,
        pickup_longitude,
        CONCAT(CAST(pickup_latitude AS STRING), ', ', CAST(pickup_longitude AS STRING)) AS pickup_location,
        dropoff_latitude,
        dropoff_longitude,
        CONCAT(CAST(dropoff_latitude AS STRING), ', ', CAST(dropoff_longitude AS STRING)) AS dropoff_location
    FROM `{PUBLIC_DATASET}`
    WHERE EXTRACT(YEAR FROM trip_start_timestamp) BETWEEN {start_year} AND {max_year}
    """
    
    print(f"Creating subset table with data from {start_year} to {max_year} ({years} year(s))...")
    print(f"  Source: {PUBLIC_DATASET}")
    print(f"  Target: {table_id}")
    
    job = client.query(query)
    job.result()  # Wait for completion
    
    # Get row count
    count_query = f"SELECT COUNT(*) as cnt FROM `{table_id}`"
    count_result = client.query(count_query).result()
    row_count = list(count_result)[0].cnt
    
    print(f"  Rows: {row_count:,}")
    
    return table_id, row_count


def create_bucket(
    storage_client: storage.Client,
    bucket_name: str,
    region: str,
) -> storage.Bucket:
    """Create a GCS bucket if it doesn't exist."""
    try:
        bucket = storage_client.get_bucket(bucket_name)
        print(f"Using existing bucket: gs://{bucket_name}")
        return bucket
    except NotFound:
        pass
    
    print(f"Creating bucket: gs://{bucket_name}")
    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "STANDARD"
    
    try:
        new_bucket = storage_client.create_bucket(bucket, location=region)
        print(f"  Location: {region}")
        return new_bucket
    except Conflict:
        # Bucket exists but we don't have access, or name taken
        print(f"  Bucket already exists or name is taken")
        return storage_client.bucket(bucket_name)


def export_to_gcs(
    bq_client: bigquery.Client,
    table_id: str,
    bucket_name: str,
    format_type: str,
) -> str:
    """Export BigQuery table to GCS in specified format."""
    # GCS destination URI with wildcard for sharding
    gcs_uri = f"gs://{bucket_name}/{format_type}/*.{format_type}"
    
    print(f"Exporting to {format_type.upper()}: {gcs_uri}")
    
    # Configure export job
    if format_type == "parquet":
        job_config = bigquery.ExtractJobConfig(
            destination_format=bigquery.DestinationFormat.PARQUET,
            compression=bigquery.Compression.SNAPPY,
        )
    else:  # avro
        job_config = bigquery.ExtractJobConfig(
            destination_format=bigquery.DestinationFormat.AVRO,
            compression=bigquery.Compression.SNAPPY,
        )
    
    # Start export job
    extract_job = bq_client.extract_table(
        table_id,
        gcs_uri,
        job_config=job_config,
    )
    
    # Wait for completion with progress
    start_time = time.time()
    extract_job.result()
    elapsed = time.time() - start_time
    
    print(f"  Completed in {elapsed:.1f}s")
    
    return f"gs://{bucket_name}/{format_type}/"


def download_from_gcs(
    storage_client: storage.Client,
    bucket_name: str,
    prefix: str,
    local_dir: Path,
) -> int:
    """Download files from GCS to local directory."""
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    if not blobs:
        print(f"  No files found in gs://{bucket_name}/{prefix}")
        return 0
    
    local_dir.mkdir(parents=True, exist_ok=True)
    
    # Clear existing files
    for existing in local_dir.glob("*"):
        if existing.is_file():
            existing.unlink()
    
    downloaded = 0
    for blob in blobs:
        if blob.name.endswith("/"):
            continue  # Skip directories
        
        filename = Path(blob.name).name
        local_path = local_dir / filename
        
        blob.download_to_filename(str(local_path))
        downloaded += 1
        
        if downloaded % 50 == 0:
            print(f"  Downloaded {downloaded}/{len(blobs)} files...")
    
    return downloaded


def cleanup_dataset(client: bigquery.Client, dataset_id: str):
    """Delete the temporary BigQuery dataset."""
    print(f"Cleaning up temporary dataset: {dataset_id}")
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


def main():
    args = parse_args()
    
    # Determine output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        # Default to taxi_data root (parent of scripts/)
        script_dir = Path(__file__).parent
        output_dir = script_dir.parent
    
    parquet_dir = output_dir / "parquet"
    avro_dir = output_dir / "avro"
    
    # Generate bucket name if not provided
    bucket_name = args.bucket
    if not bucket_name:
        short_uuid = uuid.uuid4().hex[:8]
        bucket_name = f"taxi-export-{args.project}-{short_uuid}"
    
    print("=" * 50)
    print("Chicago Taxi Data - BigQuery Export Pipeline")
    print("=" * 50)
    print(f"Project:  {args.project}")
    print(f"Years:    {args.years}")
    print(f"Bucket:   {bucket_name}")
    print(f"Region:   {args.region}")
    print(f"Output:   {output_dir}")
    print()
    
    # Initialize clients
    bq_client = bigquery.Client(project=args.project)
    storage_client = storage.Client(project=args.project)
    
    dataset_id = None
    
    try:
        # Step 1: Create temporary dataset
        dataset_id = create_dataset(bq_client, args.project, args.region)
        
        # Step 2: Create subset table
        print()
        table_id, row_count = create_subset_table(
            bq_client, dataset_id, args.years
        )
        
        if row_count == 0:
            print("Warning: No data found for the specified time range.")
            return 1
        
        # Step 3: Create GCS bucket
        print()
        create_bucket(storage_client, bucket_name, args.region)
        
        # Step 4: Export to Parquet
        print()
        export_to_gcs(bq_client, table_id, bucket_name, "parquet")
        
        # Step 5: Export to Avro
        print()
        export_to_gcs(bq_client, table_id, bucket_name, "avro")
        
        # Step 6: Download files
        print()
        print("Downloading files to local machine...")
        
        print(f"\nDownloading Parquet files to {parquet_dir}")
        parquet_count = download_from_gcs(
            storage_client, bucket_name, "parquet/", parquet_dir
        )
        
        print(f"\nDownloading Avro files to {avro_dir}")
        avro_count = download_from_gcs(
            storage_client, bucket_name, "avro/", avro_dir
        )
        
        # Summary
        print()
        print("=" * 50)
        print("Download Complete")
        print("=" * 50)
        print(f"Total rows:     {row_count:,}")
        print(f"Parquet files:  {parquet_count}")
        print(f"Avro files:     {avro_count}")
        print()
        print(f"Parquet: {parquet_dir}")
        print(f"Avro:    {avro_dir}")
        print()
        print(f"GCS bucket (retained): gs://{bucket_name}")
        
        return 0
        
    except Exception as e:
        print(f"\nError: {e}")
        raise
        
    finally:
        # Cleanup temporary dataset
        if dataset_id:
            print()
            cleanup_dataset(bq_client, dataset_id)


if __name__ == "__main__":
    sys.exit(main())
