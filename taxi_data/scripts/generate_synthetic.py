#!/usr/bin/env python3
"""
Synthetic Taxi Trip Data Generator

Generates realistic Chicago taxi trip data for testing.
Eliminates the need for GCP/BigQuery data download.

Usage:
    ./scripts/generate_synthetic.py              # 1M rows, both formats
    ./scripts/generate_synthetic.py --rows 5     # 5M rows
    ./scripts/generate_synthetic.py --format parquet  # Parquet only
"""

import argparse
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Try to import fastavro for Avro support
try:
    import fastavro
    AVRO_AVAILABLE = True
except ImportError:
    AVRO_AVAILABLE = False


# Taxi company names with relative market share weights
COMPANIES = [
    ("Taxi Affiliation Services", 18.0),
    ("Flash Cab", 17.0),
    ("Sun Taxi", 8.0),
    ("Chicago Carriage Cab Corp", 7.5),
    ("City Service", 7.0),
    ("Medallion Leasin", 6.0),
    ("Star North Management LLC", 4.0),
    ("Blue Ribbon Taxi Association Inc.", 3.5),
    ("Choice Taxi Association", 2.8),
    ("Taxi Affiliation Service Yellow", 2.8),
    ("Globe Taxi", 2.6),
    ("Chicago Independents", 2.5),
    ("Taxicab Insurance Agency, LLC", 2.0),
    ("Taxicab Insurance Agency Llc", 1.5),
    ("Yellow Cab", 1.3),
    ("Top Cab Affiliation", 1.3),
    ("Nova Taxi Affiliation Llc", 1.2),
    ("Patriot Taxi Dba Peace Taxi Associat", 1.0),
    ("24 Seven Taxi", 0.9),
    ("5 Star Taxi", 0.9),
    ("Checker Taxi", 0.8),
    ("American United Taxi Affiliation", 0.7),
    ("Metro Taxi", 0.6),
    ("Dispatch Taxi Affiliation", 0.5),
    ("Gold Coast Taxi", 0.5),
    ("Leonard Cab Co", 0.4),
    ("Chicago Taxicab", 0.4),
    ("303 Taxi", 0.3),
    ("312 Medallion Management Corp", 0.3),
    ("Suburban Dispatch LLC", 0.3),
    ("Blue Diamond", 0.25),
    ("RC Taxi", 0.25),
    ("Chicago Star Taxicab", 0.2),
    ("KOAM Taxi Association", 0.2),
    ("1085 - 72312 N and W Cab Co", 0.15),
    ("3556 - 36214 RC Andrews Cab", 0.15),
    ("4053 - 40193 Adwar H. Nikola", 0.1),
    ("0118 - 42111 Godfrey S.Awir", 0.1),
    ("2733 - 74600 Benny Jona", 0.1),
    ("3094 - 24059 G.L.B. Cab Co", 0.1),
]

PAYMENT_TYPES = ["Credit Card", "Cash", "Mobile", "Prcard", "Dispute", "No Charge"]
PAYMENT_WEIGHTS = [0.55, 0.35, 0.05, 0.03, 0.01, 0.01]

# Chicago area approximate bounds
CHICAGO_LAT_MIN, CHICAGO_LAT_MAX = 41.65, 42.02
CHICAGO_LON_MIN, CHICAGO_LON_MAX = -87.94, -87.52

# Avro schema for taxi trips
AVRO_SCHEMA = {
    "type": "record",
    "name": "TaxiTrip",
    "fields": [
        {"name": "unique_key", "type": ["null", "string"]},
        {"name": "taxi_id", "type": ["null", "string"]},
        {"name": "trip_start_timestamp", "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]},
        {"name": "trip_end_timestamp", "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]},
        {"name": "trip_seconds", "type": ["null", "long"]},
        {"name": "trip_miles", "type": ["null", "double"]},
        {"name": "pickup_census_tract", "type": ["null", "long"]},
        {"name": "dropoff_census_tract", "type": ["null", "long"]},
        {"name": "pickup_community_area", "type": ["null", "long"]},
        {"name": "dropoff_community_area", "type": ["null", "long"]},
        {"name": "fare", "type": ["null", "double"]},
        {"name": "tips", "type": ["null", "double"]},
        {"name": "tolls", "type": ["null", "double"]},
        {"name": "extras", "type": ["null", "double"]},
        {"name": "trip_total", "type": ["null", "double"]},
        {"name": "payment_type", "type": ["null", "string"]},
        {"name": "company", "type": ["null", "string"]},
        {"name": "pickup_latitude", "type": ["null", "double"]},
        {"name": "pickup_longitude", "type": ["null", "double"]},
        {"name": "pickup_location", "type": ["null", "string"]},
        {"name": "dropoff_latitude", "type": ["null", "double"]},
        {"name": "dropoff_longitude", "type": ["null", "double"]},
        {"name": "dropoff_location", "type": ["null", "string"]},
    ]
}


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate synthetic Chicago taxi trip data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                        # Generate 1M rows in both formats
  %(prog)s --rows 5               # Generate 5M rows
  %(prog)s --format parquet       # Parquet only
  %(prog)s --format avro          # Avro only
  %(prog)s --rows 1 --files 5     # 1M rows across 5 files
        """,
    )
    parser.add_argument(
        "-n", "--rows",
        type=int,
        default=1,
        help="Number of rows in millions (default: 1, max: 100)",
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=str,
        default=None,
        help="Output directory (default: taxi_data root)",
    )
    parser.add_argument(
        "-f", "--format",
        type=str,
        choices=["parquet", "avro", "both"],
        default="both",
        help="Output format (default: both)",
    )
    parser.add_argument(
        "--files",
        type=int,
        default=None,
        help="Number of output files (default: auto-calculated, ~100K rows per file)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    return parser.parse_args()


def generate_batch(batch_size: int, rng: np.random.Generator, start_date: datetime, end_date: datetime) -> dict:
    """Generate a batch of synthetic taxi trip data."""
    
    # Extract company names and weights
    company_names = [c[0] for c in COMPANIES]
    company_weights = np.array([c[1] for c in COMPANIES])
    company_weights = company_weights / company_weights.sum()
    
    # Generate unique keys
    unique_keys = [str(uuid.uuid4()).replace("-", "") for _ in range(batch_size)]
    
    # Generate taxi IDs (reusable across trips)
    taxi_ids = [f"{rng.integers(1000, 9999)}{uuid.uuid4().hex[:28]}" for _ in range(batch_size)]
    
    # Generate timestamps (random within date range)
    date_range_seconds = int((end_date - start_date).total_seconds())
    random_seconds = rng.integers(0, date_range_seconds, size=batch_size)
    trip_starts = [start_date + timedelta(seconds=int(s)) for s in random_seconds]
    
    # Trip duration: log-normal distribution (most trips 5-30 min, some longer)
    trip_seconds = rng.lognormal(mean=6.5, sigma=0.7, size=batch_size).astype(int)
    trip_seconds = np.clip(trip_seconds, 60, 7200)  # 1 min to 2 hours
    
    trip_ends = [start + timedelta(seconds=int(s)) for start, s in zip(trip_starts, trip_seconds)]
    
    # Trip miles: correlated with duration, with some variance
    base_speed_mph = rng.normal(15, 5, size=batch_size)  # Average city speed
    base_speed_mph = np.clip(base_speed_mph, 5, 40)
    trip_miles = (trip_seconds / 3600) * base_speed_mph
    trip_miles = np.round(trip_miles, 2)
    
    # Fare: base fare + per mile + per minute
    base_fare = 3.25
    per_mile = 2.25
    per_minute = 0.20
    fare = base_fare + (trip_miles * per_mile) + ((trip_seconds / 60) * per_minute)
    # Add some variance
    fare = fare * rng.normal(1.0, 0.1, size=batch_size)
    fare = np.round(np.clip(fare, 5.0, 200.0), 2)
    
    # Tips: 0-25% of fare, with many zeros (cash tips not recorded)
    tip_rate = rng.choice([0, 0, 0, 0.10, 0.15, 0.18, 0.20, 0.25], size=batch_size)
    tips = np.round(fare * tip_rate, 2)
    
    # Tolls: mostly 0, occasionally $2-5
    tolls = rng.choice([0, 0, 0, 0, 0, 0, 0, 0, 2.50, 3.00, 5.00], size=batch_size)
    
    # Extras: mostly 0, occasionally $1-3
    extras = rng.choice([0, 0, 0, 0, 0, 0, 1.0, 2.0, 3.0], size=batch_size)
    
    # Trip total
    trip_total = np.round(fare + tips + tolls + extras, 2)
    
    # Payment type
    payment_types = rng.choice(PAYMENT_TYPES, size=batch_size, p=PAYMENT_WEIGHTS)
    
    # Company
    companies = rng.choice(company_names, size=batch_size, p=company_weights)
    
    # Locations (simplified - just lat/lon)
    pickup_lat = rng.uniform(CHICAGO_LAT_MIN, CHICAGO_LAT_MAX, size=batch_size)
    pickup_lon = rng.uniform(CHICAGO_LON_MIN, CHICAGO_LON_MAX, size=batch_size)
    dropoff_lat = rng.uniform(CHICAGO_LAT_MIN, CHICAGO_LAT_MAX, size=batch_size)
    dropoff_lon = rng.uniform(CHICAGO_LON_MIN, CHICAGO_LON_MAX, size=batch_size)
    
    # Census tracts and community areas (random integers)
    pickup_census = rng.integers(17031010100, 17031990000, size=batch_size)
    dropoff_census = rng.integers(17031010100, 17031990000, size=batch_size)
    pickup_community = rng.integers(1, 78, size=batch_size)
    dropoff_community = rng.integers(1, 78, size=batch_size)
    
    return {
        "unique_key": unique_keys,
        "taxi_id": taxi_ids,
        "trip_start_timestamp": trip_starts,
        "trip_end_timestamp": trip_ends,
        "trip_seconds": trip_seconds.tolist(),
        "trip_miles": trip_miles.tolist(),
        "pickup_census_tract": pickup_census.tolist(),
        "dropoff_census_tract": dropoff_census.tolist(),
        "pickup_community_area": pickup_community.tolist(),
        "dropoff_community_area": dropoff_community.tolist(),
        "fare": fare.tolist(),
        "tips": tips.tolist(),
        "tolls": tolls.tolist(),
        "extras": extras.tolist(),
        "trip_total": trip_total.tolist(),
        "payment_type": payment_types.tolist(),
        "company": companies.tolist(),
        "pickup_latitude": np.round(pickup_lat, 6).tolist(),
        "pickup_longitude": np.round(pickup_lon, 6).tolist(),
        "pickup_location": [None] * batch_size,
        "dropoff_latitude": np.round(dropoff_lat, 6).tolist(),
        "dropoff_longitude": np.round(dropoff_lon, 6).tolist(),
        "dropoff_location": [None] * batch_size,
    }


def dict_to_table(data: dict) -> pa.Table:
    """Convert dictionary to PyArrow table with proper schema."""
    schema = pa.schema([
        ("unique_key", pa.string()),
        ("taxi_id", pa.string()),
        ("trip_start_timestamp", pa.timestamp("us")),
        ("trip_end_timestamp", pa.timestamp("us")),
        ("trip_seconds", pa.int64()),
        ("trip_miles", pa.float64()),
        ("pickup_census_tract", pa.int64()),
        ("dropoff_census_tract", pa.int64()),
        ("pickup_community_area", pa.int64()),
        ("dropoff_community_area", pa.int64()),
        ("fare", pa.float64()),
        ("tips", pa.float64()),
        ("tolls", pa.float64()),
        ("extras", pa.float64()),
        ("trip_total", pa.float64()),
        ("payment_type", pa.string()),
        ("company", pa.string()),
        ("pickup_latitude", pa.float64()),
        ("pickup_longitude", pa.float64()),
        ("pickup_location", pa.string()),
        ("dropoff_latitude", pa.float64()),
        ("dropoff_longitude", pa.float64()),
        ("dropoff_location", pa.string()),
    ])
    return pa.Table.from_pydict(data, schema=schema)


def dict_to_avro_records(data: dict) -> list:
    """Convert dictionary to list of Avro records."""
    records = []
    num_rows = len(data["unique_key"])
    
    for i in range(num_rows):
        record = {}
        for key in data:
            value = data[key][i]
            # Convert datetime to microseconds timestamp for Avro
            if isinstance(value, datetime):
                value = int(value.timestamp() * 1_000_000)
            record[key] = value
        records.append(record)
    
    return records


def clear_directory(directory: Path, extension: str):
    """Remove existing files with given extension."""
    existing_files = list(directory.glob(f"*{extension}"))
    if existing_files:
        print(f"Removing {len(existing_files)} existing {extension} file(s)...")
        for f in existing_files:
            f.unlink()


def main():
    """Main entry point."""
    args = parse_args()
    
    # Validate arguments
    if args.rows < 1:
        print("Error: --rows must be at least 1 (million)")
        return 1
    if args.rows > 100:
        print("Error: --rows cannot exceed 100 (million)")
        return 1
    
    # Check Avro availability
    generate_avro = args.format in ("avro", "both")
    generate_parquet = args.format in ("parquet", "both")
    
    if generate_avro and not AVRO_AVAILABLE:
        print("Warning: fastavro not installed. Install with: pip install fastavro")
        print("Generating Parquet only.")
        generate_avro = False
        generate_parquet = True
    
    total_rows = args.rows * 1_000_000
    
    # Determine output directory (taxi_data root)
    script_dir = Path(__file__).parent
    taxi_data_dir = script_dir.parent
    
    if args.output_dir:
        base_dir = Path(args.output_dir)
    else:
        base_dir = taxi_data_dir
    
    parquet_dir = base_dir / "parquet"
    avro_dir = base_dir / "avro"
    
    # Create output directories
    if generate_parquet:
        parquet_dir.mkdir(parents=True, exist_ok=True)
    if generate_avro:
        avro_dir.mkdir(parents=True, exist_ok=True)
    
    # Calculate number of files (target ~100K rows per file)
    rows_per_file = 100_000
    if args.files:
        num_files = args.files
        rows_per_file = total_rows // num_files
    else:
        num_files = max(1, total_rows // rows_per_file)
        rows_per_file = total_rows // num_files
    
    # Handle remainder
    remainder = total_rows - (num_files * rows_per_file)
    
    print("=" * 60)
    print("Synthetic Taxi Data Generator")
    print("=" * 60)
    print(f"Total rows: {total_rows:,}")
    print(f"Output files: {num_files}")
    print(f"Rows per file: ~{rows_per_file:,}")
    print(f"Formats: {args.format}")
    if generate_parquet:
        print(f"Parquet: {parquet_dir}")
    if generate_avro:
        print(f"Avro: {avro_dir}")
    print(f"Random seed: {args.seed}")
    print("=" * 60)
    print()
    
    # Initialize random generator
    rng = np.random.default_rng(args.seed)
    
    # Date range for trips (2019-2023)
    start_date = datetime(2019, 1, 1)
    end_date = datetime(2023, 12, 31)
    
    # Clear existing files
    if generate_parquet:
        clear_directory(parquet_dir, ".parquet")
    if generate_avro:
        clear_directory(avro_dir, ".avro")
    print()
    
    # Generate data in batches
    batch_size = min(100_000, rows_per_file)
    
    total_generated = 0
    for file_idx in range(num_files):
        file_rows = rows_per_file
        if file_idx == num_files - 1:
            file_rows += remainder
        
        # Generate data for this file
        all_data = None
        all_records = []
        rows_in_file = 0
        
        while rows_in_file < file_rows:
            batch = min(batch_size, file_rows - rows_in_file)
            data = generate_batch(batch, rng, start_date, end_date)
            
            if generate_parquet:
                table = dict_to_table(data)
                if all_data is None:
                    all_data = table
                else:
                    all_data = pa.concat_tables([all_data, table])
            
            if generate_avro:
                all_records.extend(dict_to_avro_records(data))
            
            rows_in_file += batch
        
        # Write Parquet
        if generate_parquet:
            parquet_file = parquet_dir / f"taxi_trips_synthetic_{file_idx:012d}.parquet"
            pq.write_table(all_data, parquet_file, compression="snappy")
        
        # Write Avro
        if generate_avro:
            avro_file = avro_dir / f"taxi_trips_synthetic_{file_idx:012d}.avro"
            with open(avro_file, "wb") as f:
                fastavro.writer(f, AVRO_SCHEMA, all_records, codec="snappy")
        
        total_generated += rows_in_file
        
        # Progress
        pct = (file_idx + 1) / num_files * 100
        formats = []
        if generate_parquet:
            formats.append("parquet")
        if generate_avro:
            formats.append("avro")
        print(f"[{pct:5.1f}%] Generated file {file_idx:03d} ({rows_in_file:,} rows) [{', '.join(formats)}]")
    
    print()
    print("=" * 60)
    print("Generation complete!")
    print(f"Total rows: {total_generated:,}")
    print(f"Files: {num_files}")
    if generate_parquet:
        print(f"Parquet: {parquet_dir}")
    if generate_avro:
        print(f"Avro: {avro_dir}")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
