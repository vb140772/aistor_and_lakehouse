#!/usr/bin/env python3
"""
Iceberg Internals Lab

This lab demonstrates how Iceberg table operations (INSERT, DELETE, UPDATE)
are reflected in the catalog metadata and data files using MinIO AIStor.

Learning Objectives:
1. Understand Iceberg table structure (metadata.json, manifests, data files)
2. See how INSERT creates new data files and snapshots
3. See how DELETE creates delete files or rewrites data
4. See how UPDATE works as DELETE + INSERT
5. Experience time travel queries across snapshots
"""

import time
import random
from datetime import datetime, timedelta
from typing import List, Tuple

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress
from trino.dbapi import connect

from catalog_explorer import CatalogExplorer, create_explorer

# Configuration
TRINO_HOST = "localhost"
TRINO_PORT = 9999
MINIO_HOST = "http://localhost:9000"
WAREHOUSE = "iceberglab"
NAMESPACE = "demo"
TABLE_NAME = "employees"

console = Console()


def wait_for_key(message: str = "Press any key to continue..."):
    """Wait for user to press any key."""
    import sys
    
    console.print(f"\n[dim]{message}[/dim]")
    
    # Check if stdin is a tty (interactive terminal)
    if sys.stdin.isatty():
        import tty
        import termios
        
        # Save terminal settings
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            # Set terminal to raw mode to capture single keypress
            tty.setraw(fd)
            ch = sys.stdin.read(1)
            
            # Handle Ctrl-C (ASCII 3) and Ctrl-D (ASCII 4)
            if ch == '\x03':  # Ctrl-C
                raise KeyboardInterrupt
            if ch == '\x04':  # Ctrl-D
                raise EOFError
        finally:
            # Restore terminal settings
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        
        # Print newline after keypress for cleaner output
        print()
    else:
        # Fallback for non-interactive mode (piped input)
        input()


def print_step(step_num: int, title: str, description: str):
    """Print a step header."""
    console.print()
    console.print(Panel(
        f"[bold]{description}[/bold]",
        title=f"[bold cyan]Step {step_num}: {title}[/bold cyan]",
        expand=False
    ))
    console.print()


def get_trino_connection():
    """Create a Trino connection."""
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="trino",
        catalog="iceberg_catalog",
        schema=NAMESPACE
    )


def cleanup_previous_run(explorer: CatalogExplorer, trino_conn):
    """Clean up data and metadata from previous lab runs."""
    console.print("\n[bold yellow]Cleaning up previous run data...[/bold yellow]")
    
    cur = trino_conn.cursor()
    
    # Drop Trino catalog first
    try:
        cur.execute("DROP CATALOG IF EXISTS iceberg_catalog")
        console.print("  [green]✓[/green] Dropped Trino catalog")
    except Exception as e:
        console.print(f"  [dim]No Trino catalog to drop[/dim]")
    
    # Delete tables via REST API (this properly purges from AIStor catalog)
    tables_to_drop = ["employees", "employees_cow", "employees_mor"]
    for table in tables_to_drop:
        try:
            if explorer.delete_table(WAREHOUSE, NAMESPACE, table, purge=True):
                console.print(f"  [green]✓[/green] Purged table {table} from catalog")
        except Exception as e:
            pass
    
    # Delete namespace via REST API
    try:
        if explorer.delete_namespace(WAREHOUSE, NAMESPACE):
            console.print(f"  [green]✓[/green] Deleted namespace {NAMESPACE}")
    except Exception as e:
        pass
    
    # Clean up the warehouse bucket
    try:
        files = explorer.list_files(WAREHOUSE)
        if files:
            deleted = explorer.clean_bucket(WAREHOUSE)
            console.print(f"  [green]✓[/green] Deleted {deleted} files from {WAREHOUSE} bucket")
        else:
            console.print(f"  [dim]No files in {WAREHOUSE} bucket[/dim]")
    except Exception as e:
        console.print(f"  [dim]Bucket {WAREHOUSE} does not exist yet[/dim]")
    
    console.print("[green]Cleanup complete![/green]\n")


def setup_catalog(explorer: CatalogExplorer, trino_conn) -> bool:
    """Set up the Iceberg catalog in Trino."""
    from rich.syntax import Syntax
    
    console.print("[bold yellow]Setting up Iceberg catalog...[/bold yellow]\n")
    
    # Create warehouse via REST API
    console.print(f"[bold]1. Creating warehouse '{WAREHOUSE}' via REST API:[/bold]")
    warehouse_code = f'''POST /_iceberg/v1/warehouses
Content-Type: application/json

{{"name": "{WAREHOUSE}", "upgrade-existing": true}}'''
    console.print(Syntax(warehouse_code, "http", theme="monokai", padding=1))
    explorer.create_warehouse(WAREHOUSE)
    console.print(f"  [green]✓[/green] Warehouse '{WAREHOUSE}' created\n")
    
    # Create namespace via REST API
    console.print(f"[bold]2. Creating namespace '{NAMESPACE}' via REST API:[/bold]")
    namespace_code = f'''POST /_iceberg/v1/{WAREHOUSE}/namespaces
Content-Type: application/json

{{"namespace": ["{NAMESPACE}"]}}'''
    console.print(Syntax(namespace_code, "http", theme="monokai", padding=1))
    explorer.create_namespace(WAREHOUSE, NAMESPACE)
    console.print(f"  [green]✓[/green] Namespace '{NAMESPACE}' created\n")
    
    # Create catalog in Trino
    console.print("[bold]3. Creating Iceberg catalog in Trino:[/bold]")
    
    cur = trino_conn.cursor()
    
    try:
        cur.execute("DROP CATALOG IF EXISTS iceberg_catalog")
    except:
        pass
    
    catalog_sql = f"""CREATE CATALOG iceberg_catalog USING iceberg
WITH (
    "iceberg.catalog.type" = 'rest',
    "iceberg.rest-catalog.uri" = 'http://minio:9000/_iceberg',
    "iceberg.rest-catalog.warehouse" = '{WAREHOUSE}',
    "iceberg.rest-catalog.security" = 'SIGV4',
    "iceberg.rest-catalog.vended-credentials-enabled" = 'true',
    "iceberg.unique-table-location" = 'true',
    "iceberg.rest-catalog.signing-name" = 's3tables',
    "s3.region" = 'dummy',
    "s3.endpoint" = 'http://minio:9000',
    "s3.path-style-access" = 'true',
    "fs.native-s3.enabled" = 'true'
);"""
    
    console.print(Syntax(catalog_sql, "sql", theme="monokai", padding=1))
    
    cur.execute(f"""
        CREATE CATALOG iceberg_catalog USING iceberg
        WITH (
            "iceberg.catalog.type" = 'rest',
            "iceberg.rest-catalog.uri" = 'http://minio:9000/_iceberg',
            "iceberg.rest-catalog.warehouse" = '{WAREHOUSE}',
            "iceberg.rest-catalog.security" = 'SIGV4',
            "iceberg.rest-catalog.vended-credentials-enabled" = 'true',
            "iceberg.unique-table-location" = 'true',
            "iceberg.rest-catalog.signing-name" = 's3tables',
            "s3.region" = 'dummy',
            "s3.aws-access-key" = 'minioadmin',
            "s3.aws-secret-key" = 'minioadmin',
            "s3.endpoint" = 'http://minio:9000',
            "s3.path-style-access" = 'true',
            "fs.native-s3.enabled" = 'true'
        )
    """)
    
    console.print("  [green]✓[/green] Catalog 'iceberg_catalog' created in Trino\n")
    console.print("[bold green]Setup complete![/bold green]")
    return True


def step1_create_table(explorer: CatalogExplorer, trino_conn):
    """Step 1: Create an Iceberg table and examine the metadata."""
    print_step(1, "Create Table", 
               "Create an Iceberg table and examine what gets created in the catalog and storage")
    
    cur = trino_conn.cursor()
    
    # Drop table if exists
    try:
        cur.execute(f"DROP TABLE IF EXISTS iceberg_catalog.{NAMESPACE}.{TABLE_NAME}")
    except:
        pass
    
    from rich.syntax import Syntax
    import time as time_module
    
    # Create schema if needed
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS iceberg_catalog.{NAMESPACE}")
    except:
        pass
    
    console.print(f"[bold yellow]Executing CREATE TABLE Operation[/bold yellow]\n")
    
    create_sql = f"""CREATE TABLE iceberg_catalog.{NAMESPACE}.{TABLE_NAME} (
    id          INTEGER,
    name        VARCHAR,
    department  VARCHAR,
    salary      DOUBLE,
    hire_date   DATE
);"""
    
    console.print("[bold]SQL Command:[/bold]")
    console.print(Syntax(create_sql, "sql", theme="monokai", padding=1))
    
    console.print("[bold]Execution:[/bold]")
    console.print(f"  [dim]cur = trino_conn.cursor()[/dim]")
    console.print(f"  [dim]cur.execute(sql)[/dim]")
    
    start_time = time_module.time()
    cur.execute(f"""
        CREATE TABLE iceberg_catalog.{NAMESPACE}.{TABLE_NAME} (
            id          INTEGER,
            name        VARCHAR,
            department  VARCHAR,
            salary      DOUBLE,
            hire_date   DATE
        )
    """)
    create_time = time_module.time() - start_time
    
    console.print(f"\n[bold]Result:[/bold]")
    console.print(f"  [green]✓[/green] Table '{TABLE_NAME}' created in {create_time:.3f}s")
    
    wait_for_key()
    
    # Show catalog structure
    console.print("\n[bold cyan]Catalog Structure:[/bold cyan]")
    explorer.print_catalog_structure(WAREHOUSE)
    
    wait_for_key()
    
    # Show table metadata
    console.print("\n[bold cyan]Table Metadata from Catalog:[/bold cyan]")
    metadata = explorer.get_table_metadata(WAREHOUSE, NAMESPACE, TABLE_NAME)
    if metadata:
        # Show key parts of metadata
        console.print("\n[bold]Key Metadata Fields:[/bold]")
        table = Table(show_header=True, header_style="bold")
        table.add_column("Field")
        table.add_column("Value")
        
        meta = metadata.get("metadata", {})
        table.add_row("Format Version", str(meta.get("format-version", "")))
        table.add_row("Table UUID", meta.get("table-uuid", "")[:36] + "...")
        table.add_row("Location", meta.get("location", ""))
        table.add_row("Last Sequence Number", str(meta.get("last-sequence-number", 0)))
        table.add_row("Current Snapshot ID", str(meta.get("current-snapshot-id", "None")))
        table.add_row("Total Snapshots", str(len(meta.get("snapshots", []))))
        
        console.print(table)
        
        # Show partition spec
        partition_specs = meta.get("partition-specs", [])
        if partition_specs:
            console.print("\n[bold]Partition Spec:[/bold]")
            for spec in partition_specs:
                fields = spec.get("fields", [])
                if fields:
                    console.print(f"  Spec ID {spec.get('spec-id')}: {fields}")
                else:
                    console.print(f"  Spec ID {spec.get('spec-id')}: [dim]unpartitioned[/dim]")
        
        # Show current snapshot summary (if exists)
        snapshots = meta.get("snapshots", [])
        current_snapshot_id = meta.get("current-snapshot-id")
        
        if snapshots and current_snapshot_id:
            current_snap = next((s for s in snapshots if s.get("snapshot-id") == current_snapshot_id), None)
            if current_snap:
                console.print("\n[bold]Current Snapshot Summary:[/bold]")
                summary = current_snap.get("summary", {})
                
                summary_table = Table(show_header=True, header_style="bold")
                summary_table.add_column("Metric")
                summary_table.add_column("Value", justify="right")
                
                summary_table.add_row("Operation", summary.get("operation", ""))
                summary_table.add_row("Total Data Files", summary.get("total-data-files", "0"))
                summary_table.add_row("Total Delete Files", summary.get("total-delete-files", "0"))
                summary_table.add_row("Total Records", f"{int(summary.get('total-records', 0)):,}")
                summary_table.add_row("Total Files Size", explorer._format_size(int(summary.get("total-files-size-in-bytes", 0))))
                summary_table.add_row("Added Data Files", summary.get("added-data-files", "0"))
                summary_table.add_row("Added Records", f"{int(summary.get('added-records', 0)):,}")
                
                console.print(summary_table)
                
                # Show manifest-list path
                manifest_list = current_snap.get("manifest-list", "")
                if manifest_list:
                    console.print(f"\n[bold]Manifest-List File:[/bold]")
                    console.print(f"  [dim]{manifest_list.split('/')[-1]}[/dim]")
                    
                    # Parse and show manifest-list content
                    explorer.print_manifest_list(manifest_list)
        else:
            console.print("\n[dim]No snapshots yet (empty table)[/dim]")
    
    wait_for_key()
    
    # Show files in MinIO
    console.print("\n[bold cyan]Files in MinIO Storage:[/bold cyan]")
    explorer.print_file_table(WAREHOUSE)
    
    console.print("\n[bold green]Key Observation:[/bold green]")
    console.print("  - Table created with metadata.json file")
    console.print("  - No data files yet (table is empty)")
    console.print("  - No snapshots (no data has been committed)")


def step2_insert_data(explorer: CatalogExplorer, trino_conn):
    """Step 2: Insert 1000 rows and examine the changes."""
    print_step(2, "Insert Data",
               "Insert 1000 rows and see how new data files and snapshots are created")
    
    cur = trino_conn.cursor()
    
    # Generate synthetic data
    departments = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations"]
    
    from rich.syntax import Syntax
    import time as time_module
    
    console.print("[bold]Generating 1000 employee records...[/bold]")
    
    # Build INSERT statement with VALUES
    values = []
    base_date = datetime(2020, 1, 1)
    
    for i in range(1, 1001):
        name = f"Employee_{i:04d}"
        dept = random.choice(departments)
        salary = round(50000 + random.random() * 100000, 2)
        hire_date = (base_date + timedelta(days=random.randint(0, 1500))).strftime("%Y-%m-%d")
        values.append(f"({i}, '{name}', '{dept}', {salary}, DATE '{hire_date}')")
    
    console.print(f"  [green]✓[/green] Generated 1000 rows\n")
    
    # Show INSERT command
    console.print(f"[bold yellow]Executing INSERT Operation[/bold yellow]\n")
    
    insert_sql = f"""INSERT INTO iceberg_catalog.{NAMESPACE}.{TABLE_NAME} VALUES
  (1, 'Employee_0001', 'Engineering', 75432.50, DATE '2021-03-15'),
  (2, 'Employee_0002', 'Sales', 82100.00, DATE '2020-11-22'),
  ... (998 more rows) ...
  (1000, 'Employee_1000', 'HR', 68500.75, DATE '2022-06-30');

-- All 1000 rows in a single INSERT = single atomic snapshot"""
    
    console.print("[bold]SQL Command:[/bold]")
    console.print(Syntax(insert_sql, "sql", theme="monokai", padding=1))
    
    console.print("[bold]Execution:[/bold]")
    console.print(f"  [dim]cur = trino_conn.cursor()[/dim]")
    console.print(f"  [dim]cur.execute(sql)  # 1000 rows in single statement[/dim]")
    
    start_time = time_module.time()
    with Progress() as progress:
        task = progress.add_task("[cyan]Inserting 1000 rows...", total=1)
        sql = f"INSERT INTO iceberg_catalog.{NAMESPACE}.{TABLE_NAME} VALUES {','.join(values)}"
        cur.execute(sql)
        progress.update(task, advance=1)
    insert_time = time_module.time() - start_time
    
    console.print(f"\n[bold]Result:[/bold]")
    console.print(f"  [green]✓[/green] Inserted 1000 rows in {insert_time:.3f}s (single snapshot)")
    
    wait_for_key()
    
    # Show row count
    cur.execute(f"SELECT COUNT(*) FROM iceberg_catalog.{NAMESPACE}.{TABLE_NAME}")
    count = cur.fetchone()[0]
    console.print(f"\n[bold]Table row count: {count}[/bold]")
    
    # Show sample data
    console.print("\n[bold cyan]Sample Data (first 5 rows):[/bold cyan]")
    cur.execute(f"SELECT * FROM iceberg_catalog.{NAMESPACE}.{TABLE_NAME} LIMIT 5")
    rows = cur.fetchall()
    
    table = Table(show_header=True, header_style="bold")
    table.add_column("ID")
    table.add_column("Name")
    table.add_column("Department")
    table.add_column("Salary")
    table.add_column("Hire Date")
    
    for row in rows:
        table.add_row(str(row[0]), row[1], row[2], f"${row[3]:,.2f}", str(row[4]))
    
    console.print(table)
    
    wait_for_key()
    
    # Show department breakdown
    console.print("\n[bold cyan]Rows by Department:[/bold cyan]")
    cur.execute(f"""
        SELECT department, COUNT(*) as count 
        FROM iceberg_catalog.{NAMESPACE}.{TABLE_NAME} 
        GROUP BY department 
        ORDER BY count DESC
    """)
    dept_rows = cur.fetchall()
    
    dept_table = Table(show_header=True, header_style="bold")
    dept_table.add_column("Department")
    dept_table.add_column("Count", justify="right")
    
    for row in dept_rows:
        dept_table.add_row(row[0], str(row[1]))
    
    console.print(dept_table)
    
    wait_for_key()
    
    # Show updated metadata
    console.print("\n[bold cyan]Updated Table Metadata:[/bold cyan]")
    metadata = explorer.get_table_metadata(WAREHOUSE, NAMESPACE, TABLE_NAME)
    if metadata:
        meta = metadata.get("metadata", {})
        snapshots = meta.get("snapshots", [])
        current_snapshot_id = meta.get("current-snapshot-id")
        
        console.print("\n[bold]Snapshot History:[/bold]")
        explorer.print_snapshots(metadata)
        
        # Show detailed current snapshot summary
        if snapshots and current_snapshot_id:
            current_snap = next((s for s in snapshots if s.get("snapshot-id") == current_snapshot_id), None)
            if current_snap:
                console.print("\n[bold]Current Snapshot Details:[/bold]")
                summary = current_snap.get("summary", {})
                
                summary_table = Table(show_header=True, header_style="bold")
                summary_table.add_column("Metric")
                summary_table.add_column("Value", justify="right")
                
                summary_table.add_row("Snapshot ID", str(current_snap.get("snapshot-id", "")))
                summary_table.add_row("Sequence Number", str(current_snap.get("sequence-number", "")))
                summary_table.add_row("Operation", summary.get("operation", ""))
                summary_table.add_row("Total Data Files", summary.get("total-data-files", "0"))
                summary_table.add_row("Total Delete Files", summary.get("total-delete-files", "0"))
                summary_table.add_row("Total Records", f"{int(summary.get('total-records', 0)):,}")
                summary_table.add_row("Total Files Size", explorer._format_size(int(summary.get("total-files-size-in-bytes", 0))))
                summary_table.add_row("Added Data Files", summary.get("added-data-files", "0"))
                summary_table.add_row("Added Records", f"{int(summary.get('added-records', 0)):,}")
                
                console.print(summary_table)
    
    wait_for_key()
    
    # Show manifest-list entries
    console.print("\n[bold cyan]Manifest-List Contents (Iceberg's File Index):[/bold cyan]")
    if metadata:
        manifest_list_path = explorer.get_current_manifest_list_path(metadata)
        if manifest_list_path:
            console.print(f"\n[bold]Manifest-List File:[/bold] [dim]{manifest_list_path.split('/')[-1]}[/dim]")
            console.print("[dim]This Avro file lists all manifest files that belong to this snapshot[/dim]")
            explorer.print_manifest_list(manifest_list_path)
            
            # Get the first manifest and show data files
            bucket, key = explorer._parse_s3_path(manifest_list_path)
            manifest_entries = explorer.read_manifest_list(bucket, key)
            
            if manifest_entries:
                first_manifest = manifest_entries[0]["manifest_path"]
                console.print(f"\n[bold]Manifest File Contents:[/bold] [dim]{first_manifest.split('/')[-1]}[/dim]")
                console.print("[dim]This Avro file lists individual data files with their metadata[/dim]")
                explorer.print_manifest_data_files(first_manifest, limit=5)
    
    wait_for_key()
    
    # Show files
    console.print("\n[bold cyan]Files in MinIO Storage:[/bold cyan]")
    explorer.print_file_table(WAREHOUSE)
    
    console.print("\n[bold green]Key Observations:[/bold green]")
    console.print("  - New Parquet data files created for inserted rows")
    console.print("  - [bold]Single snapshot[/bold] created for all 1000 rows (atomic commit)")
    console.print("  - Manifest-list (snap-xxx.avro) points to manifests")
    console.print("  - Manifest files (xxx-m0.avro) list individual data files")
    console.print("  - Each INSERT statement = one snapshot (batch inserts = multiple snapshots)")
    console.print("  - Tip: Use single INSERT for fewer snapshots, less metadata overhead")


def step3_delete_data(explorer: CatalogExplorer, trino_conn):
    """Step 3: Demonstrate delete operation and its effect on Iceberg files."""
    print_step(3, "Delete Data",
               "Delete rows and see how Iceberg tracks deletions with position delete files")
    
    cur = trino_conn.cursor()
    
    # Count Sales rows before delete
    cur.execute(f"SELECT COUNT(*) FROM iceberg_catalog.{NAMESPACE}.{TABLE_NAME} WHERE department = 'Sales'")
    sales_count = cur.fetchone()[0]
    
    cur.execute(f"SELECT COUNT(*) FROM iceberg_catalog.{NAMESPACE}.{TABLE_NAME}")
    total_before = cur.fetchone()[0]
    
    console.print(f"[bold]Current table status:[/bold]")
    console.print(f"  Total rows: {total_before:,}")
    console.print(f"  Sales department rows: {sales_count}")
    
    # Show metadata BEFORE delete
    console.print("\n[bold cyan]═══════════════════════════════════════════════════════════════[/bold cyan]")
    console.print("[bold cyan]              Table State BEFORE Delete                         [/bold cyan]")
    console.print("[bold cyan]═══════════════════════════════════════════════════════════════[/bold cyan]")
    
    metadata_before = explorer.get_table_metadata(WAREHOUSE, NAMESPACE, TABLE_NAME)
    files_before = explorer.count_table_files(WAREHOUSE, NAMESPACE, TABLE_NAME)
    
    if metadata_before:
        console.print("\n[bold]Snapshot History:[/bold]")
        explorer.print_snapshots(metadata_before)
        
        # Show current snapshot summary
        meta = metadata_before.get("metadata", {})
        snapshots = meta.get("snapshots", [])
        current_id = meta.get("current-snapshot-id")
        if snapshots and current_id:
            current_snap = next((s for s in snapshots if s.get("snapshot-id") == current_id), None)
            if current_snap:
                summary = current_snap.get("summary", {})
                console.print(f"\n[bold]Current Snapshot Summary:[/bold]")
                console.print(f"  Total Data Files: {summary.get('total-data-files', 0)}")
                console.print(f"  Total Records: {int(summary.get('total-records', 0)):,}")
        
        # Show manifest-list content
        manifest_list_path = explorer.get_current_manifest_list_path(metadata_before)
        if manifest_list_path:
            console.print(f"\n[bold]Manifest-List:[/bold] [dim]{manifest_list_path.split('/')[-1]}[/dim]")
            explorer.print_manifest_list(manifest_list_path)
    
    wait_for_key()
    
    # Execute DELETE
    from rich.syntax import Syntax
    import time as time_module
    
    console.print(f"\n[bold yellow]Executing DELETE Operation[/bold yellow]\n")
    
    delete_sql = f"""DELETE FROM iceberg_catalog.{NAMESPACE}.{TABLE_NAME}
WHERE department = 'Sales';"""
    
    console.print("[bold]SQL Command:[/bold]")
    console.print(Syntax(delete_sql, "sql", theme="monokai", padding=1))
    
    console.print("[bold]Execution:[/bold]")
    console.print(f"  [dim]cur = trino_conn.cursor()[/dim]")
    console.print(f"  [dim]cur.execute(sql)[/dim]")
    
    start_time = time_module.time()
    cur.execute(f"DELETE FROM iceberg_catalog.{NAMESPACE}.{TABLE_NAME} WHERE department = 'Sales'")
    delete_time = time_module.time() - start_time
    
    console.print(f"\n[bold]Result:[/bold]")
    console.print(f"  [green]✓[/green] Deleted {sales_count} rows in {delete_time:.3f}s")
    
    # Verify deletion
    cur.execute(f"SELECT COUNT(*) FROM iceberg_catalog.{NAMESPACE}.{TABLE_NAME}")
    total_after = cur.fetchone()[0]
    console.print(f"  Rows remaining: {total_after:,}")
    
    wait_for_key()
    
    # Show metadata AFTER delete
    console.print("\n[bold cyan]═══════════════════════════════════════════════════════════════[/bold cyan]")
    console.print("[bold cyan]              Table State AFTER Delete (Position Deletes)       [/bold cyan]")
    console.print("[bold cyan]═══════════════════════════════════════════════════════════════[/bold cyan]")
    
    metadata_after = explorer.get_table_metadata(WAREHOUSE, NAMESPACE, TABLE_NAME)
    files_after = explorer.count_table_files(WAREHOUSE, NAMESPACE, TABLE_NAME)
    
    if metadata_after:
        console.print("\n[bold]Snapshot History:[/bold]")
        explorer.print_snapshots(metadata_after)
        
        # Show current snapshot summary
        meta = metadata_after.get("metadata", {})
        snapshots = meta.get("snapshots", [])
        current_id = meta.get("current-snapshot-id")
        if snapshots and current_id:
            current_snap = next((s for s in snapshots if s.get("snapshot-id") == current_id), None)
            if current_snap:
                console.print("\n[bold]Current Snapshot Summary (after delete):[/bold]")
                summary = current_snap.get("summary", {})
                
                summary_table = Table(show_header=True, header_style="bold")
                summary_table.add_column("Metric")
                summary_table.add_column("Value", justify="right")
                
                summary_table.add_row("Operation", summary.get("operation", ""))
                summary_table.add_row("Total Data Files", summary.get("total-data-files", "0"))
                summary_table.add_row("Total Delete Files", f"[bold green]{summary.get('total-delete-files', '0')}[/bold green]")
                summary_table.add_row("Total Records", f"{int(summary.get('total-records', 0)):,}")
                summary_table.add_row("Added Delete Files", f"[bold green]{summary.get('added-delete-files', '0')}[/bold green]")
                summary_table.add_row("Position Deletes", f"[bold yellow]{summary.get('added-position-deletes', '0')}[/bold yellow]")
                summary_table.add_row("Total Position Deletes", summary.get("total-position-deletes", "0"))
                
                console.print(summary_table)
        
        # Show file count changes
        console.print("\n[bold]Storage File Changes:[/bold]")
        file_table = Table(show_header=True, header_style="bold")
        file_table.add_column("File Type")
        file_table.add_column("Before", justify="right")
        file_table.add_column("After", justify="right")
        file_table.add_column("Change", justify="right")
        
        data_before = files_before.get('data', 0)
        data_after = files_after.get('data', 0)
        change = data_after - data_before
        file_table.add_row("Data files (.parquet)", str(data_before), str(data_after), 
                          f"[green]+{change}[/green]" if change > 0 else str(change))
        
        man_before = files_before.get('manifest', 0)
        man_after = files_after.get('manifest', 0)
        change = man_after - man_before
        file_table.add_row("Manifest files (.avro)", str(man_before), str(man_after),
                          f"[green]+{change}[/green]" if change > 0 else str(change))
        
        console.print(file_table)
        
        # Show manifest-list content after delete
        manifest_list_path = explorer.get_current_manifest_list_path(metadata_after)
        if manifest_list_path:
            console.print(f"\n[bold]Manifest-List (after delete):[/bold]")
            explorer.print_manifest_list(manifest_list_path)
            
            # Show data files from manifest
            bucket, key = explorer._parse_s3_path(manifest_list_path)
            manifest_entries = explorer.read_manifest_list(bucket, key)
            if manifest_entries:
                for entry in manifest_entries:
                    if entry["added_data_files_count"] > 0:
                        console.print(f"\n[bold]Newly Written Data Files (without deleted rows):[/bold]")
                        explorer.print_manifest_data_files(entry["manifest_path"], limit=3)
                        break
    
    wait_for_key()
    
    # Explanation - Trino uses position delete files (merge-on-read)
    console.print("\n[bold]How DELETE works (Position Delete Files):[/bold]")
    console.print("  1. Iceberg identifies rows to delete by position in data files")
    console.print("  2. Creates a POSITION DELETE FILE (.parquet) with row positions")
    console.print("  3. Original data files remain UNCHANGED")
    console.print("  4. At read time: data files merged with delete files to exclude deleted rows")
    console.print("  5. New snapshot points to both data files and delete files")
    
    console.print("\n[bold]Delete Strategies in Iceberg:[/bold]")
    console.print("  [cyan]Position Deletes:[/cyan] Small file with row positions to skip (fast writes)")
    console.print("  [cyan]Equality Deletes:[/cyan] File with values to match and delete (schema evolution safe)")
    console.print("  [cyan]Copy-on-Write:[/cyan] Rewrite data files without deleted rows (fast reads)")
    
    console.print("\n[bold green]Key Observations:[/bold green]")
    console.print("  - DELETE creates a new snapshot (atomic operation)")
    console.print("  - Position delete files track which rows to skip")
    console.print("  - Original data files unchanged (efficient for large deletes)")
    console.print("  - Compaction merges delete files into data files periodically")


def step4_update_data(explorer: CatalogExplorer, trino_conn):
    """Step 4: Update rows and see how updates work."""
    print_step(4, "Update Data",
               "Give everyone a 10% raise and see how Iceberg handles updates")
    
    cur = trino_conn.cursor()
    
    from rich.syntax import Syntax
    import time as time_module
    
    # Show current salaries
    console.print("[bold]Current salary statistics:[/bold]")
    
    stats_sql = f"""SELECT MIN(salary), MAX(salary), AVG(salary)
FROM iceberg_catalog.{NAMESPACE}.{TABLE_NAME};"""
    console.print(Syntax(stats_sql, "sql", theme="monokai", padding=1))
    
    cur.execute(f"""
        SELECT 
            MIN(salary) as min_salary,
            MAX(salary) as max_salary,
            AVG(salary) as avg_salary
        FROM iceberg_catalog.{NAMESPACE}.{TABLE_NAME}
    """)
    stats = cur.fetchone()
    console.print(f"  Min: ${stats[0]:,.2f}")
    console.print(f"  Max: ${stats[1]:,.2f}")
    console.print(f"  Avg: ${stats[2]:,.2f}")
    
    wait_for_key()
    
    # Execute UPDATE
    console.print(f"\n[bold yellow]Executing UPDATE Operation[/bold yellow]\n")
    
    update_sql = f"""UPDATE iceberg_catalog.{NAMESPACE}.{TABLE_NAME}
SET salary = salary * 1.10;

-- This gives everyone a 10% raise"""
    
    console.print("[bold]SQL Command:[/bold]")
    console.print(Syntax(update_sql, "sql", theme="monokai", padding=1))
    
    console.print("[bold]Execution:[/bold]")
    console.print(f"  [dim]cur = trino_conn.cursor()[/dim]")
    console.print(f"  [dim]cur.execute(sql)[/dim]")
    
    start_time = time_module.time()
    cur.execute(f"""
        UPDATE iceberg_catalog.{NAMESPACE}.{TABLE_NAME}
        SET salary = salary * 1.10
    """)
    update_time = time_module.time() - start_time
    
    console.print(f"\n[bold]Result:[/bold]")
    console.print(f"  [green]✓[/green] Updated all salaries in {update_time:.3f}s")
    
    # Show new salaries
    console.print("\n[bold]New salary statistics:[/bold]")
    cur.execute(f"""
        SELECT 
            MIN(salary) as min_salary,
            MAX(salary) as max_salary,
            AVG(salary) as avg_salary
        FROM iceberg_catalog.{NAMESPACE}.{TABLE_NAME}
    """)
    new_stats = cur.fetchone()
    console.print(f"  Min: ${new_stats[0]:,.2f} (+10%)")
    console.print(f"  Max: ${new_stats[1]:,.2f} (+10%)")
    console.print(f"  Avg: ${new_stats[2]:,.2f} (+10%)")
    
    wait_for_key()
    
    # Show updated metadata
    console.print("\n[bold cyan]Updated Snapshot History:[/bold cyan]")
    metadata = explorer.get_table_metadata(WAREHOUSE, NAMESPACE, TABLE_NAME)
    if metadata:
        explorer.print_snapshots(metadata)
    
    wait_for_key()
    
    # Show files
    console.print("\n[bold cyan]Files in MinIO Storage:[/bold cyan]")
    explorer.print_file_table(WAREHOUSE)
    
    console.print("\n[bold green]Key Observations:[/bold green]")
    console.print("  - UPDATE = DELETE + INSERT in Iceberg")
    console.print("  - Creates new data files with updated values")
    console.print("  - Old data files remain for time travel")
    console.print("  - Snapshot records both deleted and added records")


def step5_time_travel(explorer: CatalogExplorer, trino_conn):
    """Step 5: Query historical data using time travel."""
    print_step(5, "Time Travel",
               "Query the table at different points in time using snapshots")
    
    cur = trino_conn.cursor()
    
    # Get snapshots
    metadata = explorer.get_table_metadata(WAREHOUSE, NAMESPACE, TABLE_NAME)
    snapshots = metadata.get("metadata", {}).get("snapshots", [])
    
    if len(snapshots) < 2:
        console.print("[yellow]Not enough snapshots for time travel demo[/yellow]")
        return
    
    console.print("[bold cyan]Available Snapshots:[/bold cyan]")
    explorer.print_snapshots(metadata)
    
    wait_for_key()
    
    # Current data
    console.print("\n[bold]Current table state:[/bold]")
    cur.execute(f"""
        SELECT COUNT(*) as count, AVG(salary) as avg_salary
        FROM iceberg_catalog.{NAMESPACE}.{TABLE_NAME}
    """)
    current = cur.fetchone()
    console.print(f"  Rows: {current[0]}")
    console.print(f"  Avg Salary: ${current[1]:,.2f}")
    
    # Query first snapshot (after initial insert, before delete)
    if len(snapshots) >= 2:
        first_snapshot_id = snapshots[0]["snapshot-id"]
        console.print(f"\n[bold]Data at first snapshot (ID: {first_snapshot_id}):[/bold]")
        
        try:
            cur.execute(f"""
                SELECT COUNT(*) as count, AVG(salary) as avg_salary
                FROM iceberg_catalog.{NAMESPACE}.{TABLE_NAME}
                FOR VERSION AS OF {first_snapshot_id}
            """)
            old = cur.fetchone()
            console.print(f"  Rows: {old[0]} (includes deleted Sales rows)")
            console.print(f"  Avg Salary: ${old[1]:,.2f} (before raises)")
        except Exception as e:
            console.print(f"[yellow]Time travel query not supported or failed: {e}[/yellow]")
    
    wait_for_key()
    
    console.print("\n[bold green]Key Observations:[/bold green]")
    console.print("  - Every operation creates an immutable snapshot")
    console.print("  - Old data files are preserved, not deleted")
    console.print("  - Query any historical state using snapshot ID")
    console.print("  - Enables: auditing, debugging, rollback")
    console.print("  - Use table maintenance to clean old snapshots")


def main():
    """Main lab execution."""
    console.print(Panel(
        "[bold]Iceberg Internals Lab[/bold]\n\n"
        "This lab demonstrates how Iceberg table operations are reflected\n"
        "in the catalog metadata and data files.\n\n"
        "You will:\n"
        "1. Create a table and examine its metadata\n"
        "2. Insert 1000 rows and see new data files\n"
        "3. Delete rows and understand copy-on-write\n"
        "4. Update rows and see delete+insert pattern\n"
        "5. Query historical data using time travel",
        title="Welcome",
        expand=False
    ))
    
    wait_for_key("Press any key to start the lab...")
    
    # Initialize
    console.print("\n[bold]Initializing...[/bold]")
    
    # Create explorer
    explorer = create_explorer(minio_host=MINIO_HOST)
    
    # Connect to Trino
    console.print("Connecting to Trino...")
    trino_conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="trino")
    
    # Clean up previous run data
    cleanup_previous_run(explorer, trino_conn)
    
    # Setup catalog
    setup_catalog(explorer, trino_conn)
    
    # Reconnect with catalog
    trino_conn = get_trino_connection()
    
    wait_for_key()
    
    # Run steps
    step1_create_table(explorer, trino_conn)
    wait_for_key("\nPress any key for Step 2...")
    
    step2_insert_data(explorer, trino_conn)
    wait_for_key("\nPress any key for Step 3...")
    
    step3_delete_data(explorer, trino_conn)
    wait_for_key("\nPress any key for Step 4...")
    
    step4_update_data(explorer, trino_conn)
    wait_for_key("\nPress any key for Step 5...")
    
    step5_time_travel(explorer, trino_conn)
    
    # Summary
    console.print("\n")
    console.print(Panel(
        "[bold green]Lab Complete![/bold green]\n\n"
        "You've learned how Iceberg manages:\n"
        "- Table metadata and catalog structure\n"
        "- Data files and manifest files\n"
        "- Snapshot isolation and atomic commits\n"
        "- Delete and update operations\n"
        "- Time travel queries\n\n"
        "To clean up: ./scripts/stop_services.sh --clean",
        title="Summary",
        expand=False
    ))


if __name__ == "__main__":
    main()
