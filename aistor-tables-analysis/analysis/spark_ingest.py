#!/usr/bin/env python3
"""
Spark Iceberg Ingestion Script
Runs inside Spark container to load Parquet files from MinIO into Iceberg table.

Usage (from docker exec):
    spark-submit --packages ... /app/analysis/spark_ingest.py \
        --warehouse trinotutorial \
        --staging-bucket trinotutorial-staging \
        --minio-host http://minio:9000
"""

import argparse
import sys
import time


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Load Parquet data into Iceberg table')
    parser.add_argument('--warehouse', default='trinotutorial', help='Iceberg warehouse name')
    parser.add_argument('--staging-bucket', default='trinotutorial-staging', help='Staging bucket with Parquet files')
    parser.add_argument('--minio-host', default='http://minio:9000', help='MinIO host URL')
    parser.add_argument('--access-key', default='minioadmin', help='MinIO access key')
    parser.add_argument('--secret-key', default='minioadmin', help='MinIO secret key')
    parser.add_argument('--schema', default='taxi_analysis', help='Schema name')
    parser.add_argument('--table', default='taxi_trips_iceberg', help='Table name')
    return parser.parse_args()


def create_spark_session(args):
    """Create Spark session configured for AIStor Iceberg catalog."""
    from pyspark.sql import SparkSession
    
    catalog_name = "tutorial_catalog"
    catalog_url = f"{args.minio_host}/_iceberg"
    
    # Build Spark session with AIStor catalog configuration
    spark = (
        SparkSession.builder
        .appName("AIStor Tables - Iceberg Ingestion")
        
        # Enable Iceberg extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        
        # Configure Iceberg catalog
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.type", "rest")
        .config(f"spark.sql.catalog.{catalog_name}.uri", catalog_url)
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", args.warehouse)
        
        # REST endpoint credentials (required for SigV4)
        .config(f"spark.sql.catalog.{catalog_name}.rest.endpoint", args.minio_host)
        .config(f"spark.sql.catalog.{catalog_name}.rest.access-key-id", args.access_key)
        .config(f"spark.sql.catalog.{catalog_name}.rest.secret-access-key", args.secret_key)
        
        # SigV4 Authentication
        .config(f"spark.sql.catalog.{catalog_name}.rest.sigv4-enabled", "true")
        .config(f"spark.sql.catalog.{catalog_name}.rest.signing-name", "s3tables")
        .config(f"spark.sql.catalog.{catalog_name}.rest.signing-region", "us-east-1")
        
        # S3 Configuration for data access
        .config(f"spark.sql.catalog.{catalog_name}.s3.access-key-id", args.access_key)
        .config(f"spark.sql.catalog.{catalog_name}.s3.secret-access-key", args.secret_key)
        .config(f"spark.sql.catalog.{catalog_name}.s3.endpoint", args.minio_host)
        .config(f"spark.sql.catalog.{catalog_name}.s3.path-style-access", "true")
        
        # S3A filesystem configuration
        .config("spark.hadoop.fs.s3a.endpoint", args.minio_host.replace("http://", "").replace("https://", ""))
        .config("spark.hadoop.fs.s3a.access.key", args.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", args.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        
        .getOrCreate()
    )
    
    return spark, catalog_name


def load_data(spark, catalog_name, args):
    """Load Parquet files from staging bucket into Iceberg table."""
    s3a_path = f"s3a://{args.staging_bucket}/parquet/"
    
    print(f"Reading Parquet files from: {s3a_path}")
    start_time = time.time()
    
    # Read Parquet files
    parquet_df = spark.read.parquet(s3a_path)
    row_count = parquet_df.count()
    print(f"Read {row_count:,} rows from Parquet files")
    
    # Create schema if needed
    print(f"Creating schema '{args.schema}' if needed...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{args.schema}")
    
    # Drop existing table
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{args.schema}.{args.table}")
    
    # Write to Iceberg table
    print(f"Creating Iceberg table '{args.table}'...")
    parquet_df.select("company", "fare").write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog_name}.{args.schema}.{args.table}")
    
    load_time = time.time() - start_time
    
    # Verify
    result_df = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog_name}.{args.schema}.{args.table}")
    table_row_count = result_df.collect()[0]["cnt"]
    
    print(f"Data loaded into Iceberg table in {load_time:.2f} seconds")
    print(f"Table contains {table_row_count:,} rows")
    
    return True


def main():
    """Main entry point."""
    args = parse_args()
    
    print("=" * 60)
    print("Spark Iceberg Ingestion")
    print("=" * 60)
    print(f"Warehouse: {args.warehouse}")
    print(f"Staging bucket: {args.staging_bucket}")
    print(f"MinIO host: {args.minio_host}")
    print(f"Schema: {args.schema}")
    print(f"Table: {args.table}")
    print("=" * 60)
    
    try:
        spark, catalog_name = create_spark_session(args)
        print(f"Spark session created (version: {spark.version})")
        
        success = load_data(spark, catalog_name, args)
        
        spark.stop()
        print("Spark session stopped")
        
        return 0 if success else 1
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
