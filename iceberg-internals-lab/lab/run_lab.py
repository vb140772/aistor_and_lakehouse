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


def wait_for_enter(message: str = "Press Enter to continue..."):
    """Wait for user to press Enter."""
    console.print(f"\n[dim]{message}[/dim]")
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


def setup_catalog(explorer: CatalogExplorer, trino_conn) -> bool:
    """Set up the Iceberg catalog in Trino."""
    console.print("[yellow]Setting up Iceberg catalog...[/yellow]")
    
    # Create warehouse
    console.print(f"  Creating warehouse '{WAREHOUSE}'...")
    explorer.create_warehouse(WAREHOUSE)
    
    # Create namespace
    console.print(f"  Creating namespace '{NAMESPACE}'...")
    explorer.create_namespace(WAREHOUSE, NAMESPACE)
    
    # Create catalog in Trino
    cur = trino_conn.cursor()
    
    try:
        cur.execute("DROP CATALOG IF EXISTS iceberg_catalog")
    except:
        pass
    
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
    
    console.print("[green]  Catalog created successfully![/green]")
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
    
    # Create schema if needed
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS iceberg_catalog.{NAMESPACE}")
    except:
        pass
    
    console.print("[bold]Creating table:[/bold]")
    console.print("""
    CREATE TABLE employees (
        id          INTEGER,
        name        VARCHAR,
        department  VARCHAR,
        salary      DOUBLE,
        hire_date   DATE
    )
    """)
    
    cur.execute(f"""
        CREATE TABLE iceberg_catalog.{NAMESPACE}.{TABLE_NAME} (
            id          INTEGER,
            name        VARCHAR,
            department  VARCHAR,
            salary      DOUBLE,
            hire_date   DATE
        )
    """)
    
    console.print("[green]Table created![/green]")
    
    wait_for_enter()
    
    # Show catalog structure
    console.print("\n[bold cyan]Catalog Structure:[/bold cyan]")
    explorer.print_catalog_structure(WAREHOUSE)
    
    wait_for_enter()
    
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
        table.add_row("Current Snapshot", str(meta.get("current-snapshot-id", "None")))
        table.add_row("Snapshots", str(len(meta.get("snapshots", []))))
        
        console.print(table)
    
    wait_for_enter()
    
    # Show files in MinIO
    console.print("\n[bold cyan]Files in MinIO Storage:[/bold cyan]")
    explorer.print_file_tree(WAREHOUSE)
    
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
    
    console.print("[bold]Generating and inserting 1000 employee records...[/bold]")
    
    # Build INSERT statement with VALUES
    values = []
    base_date = datetime(2020, 1, 1)
    
    for i in range(1, 1001):
        name = f"Employee_{i:04d}"
        dept = random.choice(departments)
        salary = round(50000 + random.random() * 100000, 2)
        hire_date = (base_date + timedelta(days=random.randint(0, 1500))).strftime("%Y-%m-%d")
        values.append(f"({i}, '{name}', '{dept}', {salary}, DATE '{hire_date}')")
    
    # Insert in batches
    batch_size = 100
    with Progress() as progress:
        task = progress.add_task("[cyan]Inserting...", total=len(values))
        
        for i in range(0, len(values), batch_size):
            batch = values[i:i+batch_size]
            sql = f"INSERT INTO iceberg_catalog.{NAMESPACE}.{TABLE_NAME} VALUES {','.join(batch)}"
            cur.execute(sql)
            progress.update(task, advance=len(batch))
    
    console.print("[green]Inserted 1000 rows![/green]")
    
    wait_for_enter()
    
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
    
    wait_for_enter()
    
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
    
    wait_for_enter()
    
    # Show updated metadata
    console.print("\n[bold cyan]Updated Table Metadata:[/bold cyan]")
    metadata = explorer.get_table_metadata(WAREHOUSE, NAMESPACE, TABLE_NAME)
    if metadata:
        console.print("\n[bold]Snapshot History:[/bold]")
        explorer.print_snapshots(metadata)
    
    wait_for_enter()
    
    # Show files
    console.print("\n[bold cyan]Files in MinIO Storage:[/bold cyan]")
    explorer.print_file_tree(WAREHOUSE)
    
    console.print("\n[bold green]Key Observations:[/bold green]")
    console.print("  - New Parquet data files created for inserted rows")
    console.print("  - New snapshot created recording the INSERT operation")
    console.print("  - Manifest files track which data files belong to each snapshot")
    console.print("  - Each INSERT is atomic - all rows committed together")


def step3_delete_data(explorer: CatalogExplorer, trino_conn):
    """Step 3: Delete rows and examine how deletes are handled."""
    print_step(3, "Delete Data",
               "Delete ~100 rows (Sales department) and see how Iceberg handles deletes")
    
    cur = trino_conn.cursor()
    
    # Count before delete
    cur.execute(f"""
        SELECT COUNT(*) FROM iceberg_catalog.{NAMESPACE}.{TABLE_NAME}
        WHERE department = 'Sales'
    """)
    sales_count = cur.fetchone()[0]
    
    console.print(f"[bold]Rows in Sales department: {sales_count}[/bold]")
    console.print("\n[bold]Executing DELETE:[/bold]")
    console.print(f"  DELETE FROM employees WHERE department = 'Sales'")
    
    wait_for_enter()
    
    # Execute delete
    cur.execute(f"""
        DELETE FROM iceberg_catalog.{NAMESPACE}.{TABLE_NAME}
        WHERE department = 'Sales'
    """)
    
    console.print(f"[green]Deleted {sales_count} rows![/green]")
    
    # Count after delete
    cur.execute(f"SELECT COUNT(*) FROM iceberg_catalog.{NAMESPACE}.{TABLE_NAME}")
    new_count = cur.fetchone()[0]
    console.print(f"\n[bold]New row count: {new_count}[/bold]")
    
    wait_for_enter()
    
    # Show updated metadata
    console.print("\n[bold cyan]Updated Snapshot History:[/bold cyan]")
    metadata = explorer.get_table_metadata(WAREHOUSE, NAMESPACE, TABLE_NAME)
    if metadata:
        explorer.print_snapshots(metadata)
    
    wait_for_enter()
    
    # Show files
    console.print("\n[bold cyan]Files in MinIO Storage:[/bold cyan]")
    explorer.print_file_tree(WAREHOUSE)
    
    console.print("\n[bold green]Key Observations:[/bold green]")
    console.print("  - DELETE creates a new snapshot")
    console.print("  - Iceberg uses 'copy-on-write' by default:")
    console.print("    - Rewrites data files without deleted rows")
    console.print("    - Old data files are still present (for time travel)")
    console.print("  - Alternative: 'merge-on-read' uses delete files")
    console.print("    - Faster deletes, slower reads")


def step4_update_data(explorer: CatalogExplorer, trino_conn):
    """Step 4: Update rows and see how updates work."""
    print_step(4, "Update Data",
               "Give everyone a 10% raise and see how Iceberg handles updates")
    
    cur = trino_conn.cursor()
    
    # Show current salaries
    console.print("[bold]Current salary statistics:[/bold]")
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
    
    console.print("\n[bold]Executing UPDATE:[/bold]")
    console.print("  UPDATE employees SET salary = salary * 1.10")
    
    wait_for_enter()
    
    # Execute update
    cur.execute(f"""
        UPDATE iceberg_catalog.{NAMESPACE}.{TABLE_NAME}
        SET salary = salary * 1.10
    """)
    
    console.print("[green]Updated all salaries![/green]")
    
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
    
    wait_for_enter()
    
    # Show updated metadata
    console.print("\n[bold cyan]Updated Snapshot History:[/bold cyan]")
    metadata = explorer.get_table_metadata(WAREHOUSE, NAMESPACE, TABLE_NAME)
    if metadata:
        explorer.print_snapshots(metadata)
    
    wait_for_enter()
    
    # Show files
    console.print("\n[bold cyan]Files in MinIO Storage:[/bold cyan]")
    explorer.print_file_tree(WAREHOUSE)
    
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
    
    wait_for_enter()
    
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
    
    wait_for_enter()
    
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
    
    wait_for_enter("Press Enter to start the lab...")
    
    # Initialize
    console.print("\n[bold]Initializing...[/bold]")
    
    # Create explorer
    explorer = create_explorer(minio_host=MINIO_HOST)
    
    # Connect to Trino
    console.print("Connecting to Trino...")
    trino_conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="trino")
    
    # Setup catalog
    setup_catalog(explorer, trino_conn)
    
    # Reconnect with catalog
    trino_conn = get_trino_connection()
    
    wait_for_enter()
    
    # Run steps
    step1_create_table(explorer, trino_conn)
    wait_for_enter("\nPress Enter for Step 2...")
    
    step2_insert_data(explorer, trino_conn)
    wait_for_enter("\nPress Enter for Step 3...")
    
    step3_delete_data(explorer, trino_conn)
    wait_for_enter("\nPress Enter for Step 4...")
    
    step4_update_data(explorer, trino_conn)
    wait_for_enter("\nPress Enter for Step 5...")
    
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
