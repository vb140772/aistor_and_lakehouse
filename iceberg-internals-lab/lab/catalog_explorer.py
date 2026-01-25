"""
AIStor Catalog Explorer

Helper functions to explore the Iceberg catalog metadata and data files
stored in MinIO AIStor.
"""

import io
import json
import requests
import boto3
from typing import List, Dict, Any, Optional
from tabulate import tabulate
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.tree import Tree
import fastavro

try:
    from . import sigv4
except ImportError:
    import sigv4

# Default configuration
DEFAULT_MINIO_HOST = "http://localhost:9000"
DEFAULT_ACCESS_KEY = "minioadmin"
DEFAULT_SECRET_KEY = "minioadmin"
DEFAULT_REGION = "dummy"

console = Console()


class CatalogExplorer:
    """Explore AIStor Iceberg catalog metadata and data files."""
    
    def __init__(
        self,
        minio_host: str = DEFAULT_MINIO_HOST,
        access_key: str = DEFAULT_ACCESS_KEY,
        secret_key: str = DEFAULT_SECRET_KEY,
        region: str = DEFAULT_REGION
    ):
        self.minio_host = minio_host
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.catalog_url = f"{minio_host}/_iceberg"
        
        # S3 client for file operations
        self.s3 = boto3.client(
            's3',
            endpoint_url=minio_host,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
    
    def _signed_request(self, method: str, url: str, body: str = "", headers: Dict = None) -> requests.Response:
        """Make a signed request to the AIStor catalog API."""
        if headers is None:
            headers = {}
        
        aws_sign = sigv4.sign(
            method=method,
            url=url,
            body=body,
            host=self.minio_host,
            headers=headers.copy(),
            access_key=self.access_key,
            secret_key=self.secret_key
        )
        
        if method == "GET":
            return requests.get(url, headers=aws_sign.headers, timeout=30)
        elif method == "POST":
            return requests.post(url, data=body, headers=aws_sign.headers, timeout=30)
        elif method == "DELETE":
            return requests.delete(url, headers=aws_sign.headers, timeout=30)
        else:
            raise ValueError(f"Unsupported method: {method}")
    
    # ========================================
    # Warehouse Operations
    # ========================================
    
    def list_warehouses(self) -> List[str]:
        """List all warehouses in the catalog."""
        url = f"{self.catalog_url}/v1/warehouses"
        response = self._signed_request("GET", url)
        
        if response.status_code == 200:
            data = response.json()
            return data.get("warehouses", [])
        return []
    
    def create_warehouse(self, name: str, upgrade_existing: bool = True) -> bool:
        """Create a warehouse (bucket) for Iceberg tables."""
        url = f"{self.catalog_url}/v1/warehouses"
        payload = json.dumps({"name": name, "upgrade-existing": upgrade_existing})
        headers = {
            "content-type": "application/json",
            "content-length": str(len(payload))
        }
        
        response = self._signed_request("POST", url, payload, headers)
        return response.status_code in [200, 201, 409]
    
    # ========================================
    # Namespace Operations
    # ========================================
    
    def list_namespaces(self, warehouse: str) -> List[str]:
        """List all namespaces in a warehouse."""
        url = f"{self.catalog_url}/v1/{warehouse}/namespaces"
        response = self._signed_request("GET", url)
        
        if response.status_code == 200:
            data = response.json()
            namespaces = data.get("namespaces", [])
            return [ns[0] if isinstance(ns, list) else ns for ns in namespaces]
        return []
    
    def create_namespace(self, warehouse: str, namespace: str) -> bool:
        """Create a namespace in the warehouse."""
        url = f"{self.catalog_url}/v1/{warehouse}/namespaces"
        payload = json.dumps({"namespace": [namespace]})
        headers = {
            "content-type": "application/json",
            "content-length": str(len(payload))
        }
        
        response = self._signed_request("POST", url, payload, headers)
        return response.status_code in [200, 201, 409]
    
    # ========================================
    # Table Operations
    # ========================================
    
    def list_tables(self, warehouse: str, namespace: str) -> List[str]:
        """List all tables in a namespace."""
        url = f"{self.catalog_url}/v1/{warehouse}/namespaces/{namespace}/tables"
        response = self._signed_request("GET", url)
        
        if response.status_code == 200:
            data = response.json()
            identifiers = data.get("identifiers", [])
            return [t.get("name", "") for t in identifiers]
        return []
    
    def get_table_metadata(self, warehouse: str, namespace: str, table: str) -> Optional[Dict]:
        """Get table metadata from the catalog."""
        url = f"{self.catalog_url}/v1/{warehouse}/namespaces/{namespace}/tables/{table}"
        response = self._signed_request("GET", url)
        
        if response.status_code == 200:
            return response.json()
        return None
    
    def delete_table(self, warehouse: str, namespace: str, table: str, purge: bool = True) -> bool:
        """Delete a table from the catalog via REST API.
        
        Args:
            warehouse: Warehouse name
            namespace: Namespace name
            table: Table name
            purge: If True, also purge table data (default True)
            
        Returns:
            True if successful
        """
        url = f"{self.catalog_url}/v1/{warehouse}/namespaces/{namespace}/tables/{table}"
        if purge:
            url += "?purgeRequested=true"
        
        response = self._signed_request("DELETE", url)
        return response.status_code in [200, 204, 404]  # 404 = already deleted
    
    def delete_namespace(self, warehouse: str, namespace: str) -> bool:
        """Delete a namespace from the catalog via REST API.
        
        Args:
            warehouse: Warehouse name
            namespace: Namespace name
            
        Returns:
            True if successful
        """
        url = f"{self.catalog_url}/v1/{warehouse}/namespaces/{namespace}"
        response = self._signed_request("DELETE", url)
        return response.status_code in [200, 204, 404]
    
    # ========================================
    # File System Operations (MinIO/S3)
    # ========================================
    
    def list_buckets(self) -> List[str]:
        """List all buckets in MinIO."""
        response = self.s3.list_buckets()
        return [b['Name'] for b in response.get('Buckets', [])]
    
    def list_files(self, bucket: str, prefix: str = "") -> List[Dict]:
        """List files in a bucket with optional prefix."""
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            files = []
            
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat()
                    })
            
            return files
        except Exception as e:
            console.print(f"[red]Error listing files: {e}[/red]")
            return []
    
    def clean_bucket(self, bucket: str, prefix: str = "") -> int:
        """Delete all objects in a bucket (or under a prefix).
        
        Args:
            bucket: Bucket name
            prefix: Optional prefix to limit deletion scope
            
        Returns:
            Number of objects deleted
        """
        try:
            files = self.list_files(bucket, prefix)
            if not files:
                return 0
            
            # Delete objects in batches of 1000 (S3 limit)
            deleted = 0
            for i in range(0, len(files), 1000):
                batch = files[i:i+1000]
                delete_objects = [{'Key': f['key']} for f in batch]
                
                self.s3.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects': delete_objects}
                )
                deleted += len(batch)
            
            return deleted
        except Exception as e:
            console.print(f"[red]Error cleaning bucket: {e}[/red]")
            return 0
    
    def delete_bucket(self, bucket: str) -> bool:
        """Delete a bucket (must be empty first).
        
        Args:
            bucket: Bucket name
            
        Returns:
            True if successful
        """
        try:
            # First clean all objects
            self.clean_bucket(bucket)
            # Then delete the bucket
            self.s3.delete_bucket(Bucket=bucket)
            return True
        except Exception as e:
            console.print(f"[red]Error deleting bucket: {e}[/red]")
            return False
    
    def get_file_content(self, bucket: str, key: str) -> Optional[bytes]:
        """Get file content from MinIO."""
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key)
            return response['Body'].read()
        except Exception as e:
            console.print(f"[red]Error reading file: {e}[/red]")
            return None
    
    def get_json_file(self, bucket: str, key: str) -> Optional[Dict]:
        """Get and parse a JSON file from MinIO."""
        content = self.get_file_content(bucket, key)
        if content:
            try:
                return json.loads(content.decode('utf-8'))
            except json.JSONDecodeError:
                return None
        return None
    
    # ========================================
    # Avro Manifest Parsing
    # ========================================
    
    def read_manifest_list(self, bucket: str, key: str) -> List[Dict]:
        """Parse an Iceberg manifest-list Avro file.
        
        Returns list of manifest entries with:
        - manifest_path: Path to the manifest file
        - manifest_length: Size of the manifest file in bytes
        - added_data_files_count: Number of data files added by this manifest
        - deleted_data_files_count: Number of data files deleted by this manifest
        - partition_spec_id: Partition spec ID used
        - added_rows_count: Number of rows added
        - existing_rows_count: Number of existing rows
        - deleted_rows_count: Number of rows deleted
        """
        content = self.get_file_content(bucket, key)
        if not content:
            return []
        
        try:
            reader = fastavro.reader(io.BytesIO(content))
            entries = []
            
            for record in reader:
                entry = {
                    "manifest_path": record.get("manifest_path", ""),
                    "manifest_length": record.get("manifest_length", 0),
                    "partition_spec_id": record.get("partition_spec_id", 0),
                    "added_data_files_count": record.get("added_data_files_count", 0),
                    "existing_data_files_count": record.get("existing_data_files_count", 0),
                    "deleted_data_files_count": record.get("deleted_data_files_count", 0),
                    "added_rows_count": record.get("added_rows_count", 0),
                    "existing_rows_count": record.get("existing_rows_count", 0),
                    "deleted_rows_count": record.get("deleted_rows_count", 0),
                    "content": record.get("content", 0),  # 0 = data, 1 = deletes
                }
                entries.append(entry)
            
            return entries
        except Exception as e:
            console.print(f"[red]Error parsing manifest list: {e}[/red]")
            return []
    
    def read_manifest(self, bucket: str, key: str) -> List[Dict]:
        """Parse an Iceberg manifest Avro file.
        
        Returns list of data file entries with:
        - file_path: Path to the data file
        - file_format: Format of the file (PARQUET, ORC, AVRO)
        - record_count: Number of records in the file
        - file_size_bytes: Size of the file in bytes
        - partition: Partition values (if any)
        - status: 0=existing, 1=added, 2=deleted
        """
        content = self.get_file_content(bucket, key)
        if not content:
            return []
        
        try:
            reader = fastavro.reader(io.BytesIO(content))
            entries = []
            
            for record in reader:
                # Status: 0 = existing, 1 = added, 2 = deleted
                status = record.get("status", 0)
                status_str = {0: "existing", 1: "added", 2: "deleted"}.get(status, "unknown")
                
                data_file = record.get("data_file", {})
                
                # Handle file format - can be string or int
                file_format = data_file.get("file_format", "PARQUET")
                if isinstance(file_format, int):
                    file_format = {0: "AVRO", 1: "ORC", 2: "PARQUET"}.get(file_format, "UNKNOWN")
                
                # Handle content type - 0 = data, 1 = position deletes, 2 = equality deletes
                content_type = data_file.get("content", 0)
                content_str = {0: "data", 1: "position_deletes", 2: "equality_deletes"}.get(content_type, "data")
                
                entry = {
                    "file_path": data_file.get("file_path", ""),
                    "file_format": file_format,
                    "record_count": data_file.get("record_count", 0),
                    "file_size_bytes": data_file.get("file_size_in_bytes", 0),
                    "partition": data_file.get("partition", {}),
                    "status": status_str,
                    "content": content_str,
                    "column_sizes": data_file.get("column_sizes", {}),
                    "value_counts": data_file.get("value_counts", {}),
                    "null_value_counts": data_file.get("null_value_counts", {}),
                }
                entries.append(entry)
            
            return entries
        except Exception as e:
            console.print(f"[red]Error parsing manifest: {e}[/red]")
            return []
    
    def _parse_s3_path(self, path: str) -> tuple:
        """Parse an S3 path into bucket and key.
        
        Handles s3://, s3a://, and plain paths.
        Returns (bucket, key) tuple.
        """
        if path.startswith("s3://"):
            path = path[5:]
        elif path.startswith("s3a://"):
            path = path[6:]
        
        parts = path.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key
    
    # ========================================
    # Display Functions
    # ========================================
    
    def print_catalog_structure(self, warehouse: str):
        """Print the catalog structure as a tree."""
        tree = Tree(f"[bold blue]{warehouse}[/bold blue] (warehouse)")
        
        namespaces = self.list_namespaces(warehouse)
        for ns in namespaces:
            ns_branch = tree.add(f"[yellow]{ns}[/yellow] (namespace)")
            tables = self.list_tables(warehouse, ns)
            for table in tables:
                ns_branch.add(f"[green]{table}[/green] (table)")
        
        console.print(tree)
    
    def print_file_tree(self, bucket: str, prefix: str = ""):
        """Print files in bucket as a tree."""
        files = self.list_files(bucket, prefix)
        
        tree = Tree(f"[bold blue]s3://{bucket}/{prefix}[/bold blue]")
        
        # Group files by directory
        dirs = {}
        for f in files:
            parts = f['key'].split('/')
            if len(parts) > 1:
                dir_name = '/'.join(parts[:-1])
                if dir_name not in dirs:
                    dirs[dir_name] = []
                dirs[dir_name].append({
                    'name': parts[-1],
                    'size': f['size']
                })
            else:
                tree.add(f"[cyan]{f['key']}[/cyan] ({self._format_size(f['size'])})")
        
        for dir_name, dir_files in sorted(dirs.items()):
            dir_branch = tree.add(f"[yellow]{dir_name}/[/yellow]")
            for df in dir_files:
                dir_branch.add(f"[cyan]{df['name']}[/cyan] ({self._format_size(df['size'])})")
        
        console.print(tree)
    
    def print_metadata_json(self, data: Dict, title: str = "Metadata"):
        """Pretty print JSON metadata."""
        json_str = json.dumps(data, indent=2, default=str)
        syntax = Syntax(json_str, "json", theme="monokai", line_numbers=True)
        console.print(Panel(syntax, title=title, expand=False))
    
    def print_snapshots(self, metadata: Dict, show_delete_files: bool = False):
        """Print snapshot history from table metadata."""
        snapshots = metadata.get("metadata", {}).get("snapshots", [])
        
        if not snapshots:
            console.print("[yellow]No snapshots found[/yellow]")
            return
        
        table_data = []
        headers = ["Snapshot ID", "Timestamp", "Operation", "Added Files", "Deleted Files", "Added Rows", "Deleted Rows"]
        
        if show_delete_files:
            headers.extend(["Added Delete Files", "Equality Deletes", "Position Deletes"])
        
        for snap in snapshots:
            summary = snap.get("summary", {})
            
            # Calculate deleted rows: position deletes + equality deletes
            # Note: deleted-records might not be populated, use position/equality deletes instead
            deleted_records = summary.get("deleted-records", "0")
            position_deletes = summary.get("added-position-deletes", "0")
            equality_deletes = summary.get("added-equality-deletes", "0")
            
            # Use position deletes if deleted-records is 0
            if deleted_records == "0" or deleted_records == 0:
                total_deletes = int(position_deletes or 0) + int(equality_deletes or 0)
                deleted_rows = str(total_deletes) if total_deletes > 0 else "0"
            else:
                deleted_rows = str(deleted_records)
            
            row = [
                snap.get("snapshot-id", ""),
                snap.get("timestamp-ms", ""),
                summary.get("operation", ""),
                summary.get("added-data-files", "0"),
                summary.get("deleted-data-files", "0"),
                summary.get("added-records", "0"),
                deleted_rows,
            ]
            
            if show_delete_files:
                row.extend([
                    summary.get("added-delete-files", "0"),
                    summary.get("added-equality-deletes", "0"),
                    summary.get("added-position-deletes", "0"),
                ])
            
            table_data.append(row)
        
        console.print(tabulate(table_data, headers=headers, tablefmt="simple"))
    
    def print_snapshot_file_stats(self, metadata: Dict):
        """Print file statistics from snapshot summary.
        
        Shows: total-data-files, total-delete-files, total-records, total-files-size
        """
        snapshots = metadata.get("metadata", {}).get("snapshots", [])
        current_snapshot_id = metadata.get("metadata", {}).get("current-snapshot-id")
        
        if not snapshots:
            console.print("[yellow]No snapshots found[/yellow]")
            return
        
        # Find current snapshot
        current_snapshot = None
        for snap in snapshots:
            if snap.get("snapshot-id") == current_snapshot_id:
                current_snapshot = snap
                break
        
        if not current_snapshot:
            current_snapshot = snapshots[-1]  # Use last snapshot
        
        summary = current_snapshot.get("summary", {})
        
        console.print("\n[bold]Snapshot File Statistics:[/bold]")
        
        from rich.table import Table as RichTable
        table = RichTable(show_header=True, header_style="bold")
        table.add_column("Metric")
        table.add_column("Value", justify="right")
        
        total_data_files = summary.get("total-data-files", "0")
        total_delete_files = summary.get("total-delete-files", "0")
        total_records = summary.get("total-records", "0")
        total_files_size = int(summary.get("total-files-size-in-bytes", 0))
        
        table.add_row("Total Data Files", str(total_data_files))
        table.add_row("Total Delete Files", str(total_delete_files))
        table.add_row("Total Records", f"{int(total_records):,}")
        table.add_row("Total Files Size", self._format_size(total_files_size))
        
        console.print(table)
    
    def print_manifest_list(self, manifest_list_path: str):
        """Display manifest list entries from an Avro manifest-list file.
        
        Args:
            manifest_list_path: S3 path to the manifest-list file (snap-xxx.avro)
        """
        bucket, key = self._parse_s3_path(manifest_list_path)
        entries = self.read_manifest_list(bucket, key)
        
        if not entries:
            console.print("[yellow]No manifest entries found[/yellow]")
            return
        
        console.print(f"\n[bold]Manifest List Entries ({len(entries)} manifests):[/bold]")
        
        from rich.table import Table as RichTable
        table = RichTable(show_header=True, header_style="bold")
        table.add_column("Manifest File")
        table.add_column("Size", justify="right")
        table.add_column("Content")
        table.add_column("Added Files", justify="right")
        table.add_column("Existing", justify="right")
        table.add_column("Deleted", justify="right")
        table.add_column("Added Rows", justify="right")
        
        for entry in entries:
            # Extract just the filename from the path
            manifest_name = entry["manifest_path"].split("/")[-1]
            content_type = "data" if entry["content"] == 0 else "deletes"
            
            table.add_row(
                manifest_name,
                self._format_size(entry["manifest_length"]),
                content_type,
                str(entry["added_data_files_count"]),
                str(entry["existing_data_files_count"]),
                str(entry["deleted_data_files_count"]),
                f"{entry['added_rows_count']:,}"
            )
        
        console.print(table)
    
    def print_manifest_data_files(self, manifest_path: str, limit: int = 10):
        """Display data file entries from an Avro manifest file.
        
        Args:
            manifest_path: S3 path to the manifest file
            limit: Maximum number of entries to display
        """
        bucket, key = self._parse_s3_path(manifest_path)
        entries = self.read_manifest(bucket, key)
        
        if not entries:
            console.print("[yellow]No data file entries found[/yellow]")
            return
        
        console.print(f"\n[bold]Data File Entries ({len(entries)} files, showing {min(limit, len(entries))}):[/bold]")
        
        from rich.table import Table as RichTable
        table = RichTable(show_header=True, header_style="bold")
        table.add_column("File Name")
        table.add_column("Format")
        table.add_column("Content")
        table.add_column("Status")
        table.add_column("Records", justify="right")
        table.add_column("Size", justify="right")
        
        for entry in entries[:limit]:
            # Extract just the filename from the path
            file_name = entry["file_path"].split("/")[-1]
            if len(file_name) > 40:
                file_name = file_name[:37] + "..."
            
            status_style = {
                "added": "[green]added[/green]",
                "existing": "[dim]existing[/dim]",
                "deleted": "[red]deleted[/red]"
            }.get(entry["status"], entry["status"])
            
            content_style = {
                "data": "data",
                "position_deletes": "[yellow]pos-del[/yellow]",
                "equality_deletes": "[yellow]eq-del[/yellow]"
            }.get(entry["content"], entry["content"])
            
            table.add_row(
                file_name,
                entry["file_format"],
                content_style,
                status_style,
                f"{entry['record_count']:,}",
                self._format_size(entry["file_size_bytes"])
            )
        
        if len(entries) > limit:
            console.print(f"[dim]... and {len(entries) - limit} more files[/dim]")
        
        console.print(table)
    
    def get_current_manifest_list_path(self, metadata: Dict) -> Optional[str]:
        """Get the manifest-list path from current snapshot.
        
        Args:
            metadata: Table metadata dict from get_table_metadata()
            
        Returns:
            S3 path to the manifest-list file, or None
        """
        snapshots = metadata.get("metadata", {}).get("snapshots", [])
        current_snapshot_id = metadata.get("metadata", {}).get("current-snapshot-id")
        
        for snap in snapshots:
            if snap.get("snapshot-id") == current_snapshot_id:
                return snap.get("manifest-list")
        
        # Return last snapshot's manifest-list if current not found
        if snapshots:
            return snapshots[-1].get("manifest-list")
        
        return None
    
    def count_table_files(self, warehouse: str, namespace: str, table: str) -> Dict[str, int]:
        """Count different types of files for an Iceberg table.
        
        Returns dict with counts of:
        - data: Parquet data files
        - delete: Delete files (for merge-on-read)
        - manifest: Manifest and manifest list files
        - metadata: Metadata JSON files
        """
        # Get table metadata to find the table location
        metadata = self.get_table_metadata(warehouse, namespace, table)
        if not metadata:
            return {"data": 0, "delete": 0, "manifest": 0, "metadata": 0}
        
        location = metadata.get("metadata", {}).get("location", "")
        
        # Parse the S3 path to get bucket and prefix
        # Format: s3://bucket/prefix or s3a://bucket/prefix
        if location.startswith("s3://"):
            path = location[5:]
        elif location.startswith("s3a://"):
            path = location[6:]
        else:
            # Try to find files in warehouse bucket with table name
            path = f"{warehouse}/{namespace}/{table}"
        
        parts = path.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        
        # List all files under the table location
        files = self.list_files(bucket, prefix)
        
        counts = {
            "data": 0,
            "delete": 0,
            "manifest": 0,
            "metadata": 0,
            "total_data_size": 0,
            "total_delete_size": 0
        }
        
        for f in files:
            key = f['key'].lower()
            size = f.get('size', 0)
            
            if '/data/' in key and key.endswith('.parquet'):
                # Check if it's a delete file (contains -deletes- in name or in delete folder)
                if '-deletes-' in key or '/deletes/' in key:
                    counts["delete"] += 1
                    counts["total_delete_size"] += size
                else:
                    counts["data"] += 1
                    counts["total_data_size"] += size
            elif key.endswith('.avro'):
                # Manifest files are Avro format
                counts["manifest"] += 1
            elif key.endswith('.json') or key.endswith('metadata.json'):
                counts["metadata"] += 1
        
        return counts
    
    def get_table_file_details(self, warehouse: str, namespace: str, table: str) -> Dict[str, List[Dict]]:
        """Get detailed file information for an Iceberg table.
        
        Returns dict with lists of file details:
        - data_files: List of data file info
        - delete_files: List of delete file info  
        - manifests: List of manifest file info
        """
        metadata = self.get_table_metadata(warehouse, namespace, table)
        if not metadata:
            return {"data_files": [], "delete_files": [], "manifests": []}
        
        location = metadata.get("metadata", {}).get("location", "")
        
        if location.startswith("s3://"):
            path = location[5:]
        elif location.startswith("s3a://"):
            path = location[6:]
        else:
            path = f"{warehouse}/{namespace}/{table}"
        
        parts = path.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        
        files = self.list_files(bucket, prefix)
        
        result = {
            "data_files": [],
            "delete_files": [],
            "manifests": []
        }
        
        for f in files:
            key = f['key']
            key_lower = key.lower()
            
            file_info = {
                "path": f"s3://{bucket}/{key}",
                "name": key.split("/")[-1],
                "size": f['size'],
                "size_formatted": self._format_size(f['size'])
            }
            
            if '/data/' in key_lower and key_lower.endswith('.parquet'):
                if '-deletes-' in key_lower or '/deletes/' in key_lower:
                    result["delete_files"].append(file_info)
                else:
                    result["data_files"].append(file_info)
            elif key_lower.endswith('.avro'):
                result["manifests"].append(file_info)
        
        return result
    
    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"
    
    def _classify_iceberg_file(self, filename: str, full_path: str) -> tuple:
        """Classify an Iceberg file by type and provide educational comment.
        
        Args:
            filename: Just the file name
            full_path: Full path including directories
            
        Returns:
            Tuple of (type, comment) describing the file's purpose
        """
        filename_lower = filename.lower()
        path_lower = full_path.lower()
        
        # Metadata JSON files
        if filename_lower.endswith('.metadata.json') or 'metadata.json' in filename_lower:
            return ("Metadata", "Table state: schema, partitions, snapshot refs")
        
        # Manifest-list (snapshot) files - snap-*.avro
        if filename_lower.startswith('snap-') and filename_lower.endswith('.avro'):
            return ("Manifest-List", "Snapshot index pointing to manifest files")
        
        # Manifest files - *-m*.avro (but not snap-*)
        if filename_lower.endswith('.avro') and '-m' in filename_lower:
            return ("Manifest", "Data file index with stats and partitions")
        
        # Other avro files (could be manifests with different naming)
        if filename_lower.endswith('.avro'):
            return ("Manifest", "Avro manifest or manifest-list file")
        
        # Delete files (position or equality deletes)
        if filename_lower.endswith('.parquet'):
            if '-delete-' in filename_lower or '-deletes-' in filename_lower or '/deletes/' in path_lower:
                return ("Delete File", "Marks deleted rows (merge-on-read)")
            # Regular data files
            if '/data/' in path_lower:
                return ("Data File", "Row data in columnar Parquet format")
            return ("Parquet", "Parquet file")
        
        # Version hint file
        if 'version-hint' in filename_lower:
            return ("Version Hint", "Points to current metadata version")
        
        # Unknown
        return ("File", "")
    
    def print_file_table(self, bucket: str, prefix: str = ""):
        """Print files in bucket as a table with tree structure, Type and Comment columns.
        
        Args:
            bucket: S3 bucket name
            prefix: Optional prefix to filter files
        """
        files = self.list_files(bucket, prefix)
        
        if not files:
            console.print("[yellow]No files found[/yellow]")
            return
        
        console.print(f"\n[bold]Files in s3://{bucket}/[/bold]")
        
        from rich.table import Table as RichTable
        table = RichTable(show_header=True, header_style="bold", box=None, expand=True)
        table.add_column("Size", justify="right", width=10)
        table.add_column("Type", width=14)
        table.add_column("Comment", width=45)
        table.add_column("File Structure", ratio=1)  # Takes remaining space
        
        # Sort files by path for better organization
        sorted_files = sorted(files, key=lambda x: x['key'])
        
        # Build tree structure
        # Group files by their directory structure
        tree_items = []  # List of (indent_level, is_last, is_dir, name, file_info)
        
        # Track directories we've seen
        seen_dirs = set()
        
        for i, f in enumerate(sorted_files):
            full_path = f['key']
            parts = full_path.split('/')
            
            # Add directory entries we haven't seen yet
            for depth in range(len(parts) - 1):
                dir_path = '/'.join(parts[:depth + 1])
                if dir_path not in seen_dirs:
                    seen_dirs.add(dir_path)
                    tree_items.append({
                        'depth': depth,
                        'name': parts[depth],
                        'is_dir': True,
                        'file_info': None,
                        'full_path': dir_path
                    })
            
            # Add the file
            tree_items.append({
                'depth': len(parts) - 1,
                'name': parts[-1],
                'is_dir': False,
                'file_info': f,
                'full_path': full_path
            })
        
        # Render tree with proper connectors
        for idx, item in enumerate(tree_items):
            depth = item['depth']
            name = item['name']
            is_dir = item['is_dir']
            
            # Determine if this is the last item at this depth level
            is_last_at_depth = True
            for future_item in tree_items[idx + 1:]:
                if future_item['depth'] < depth:
                    break
                if future_item['depth'] == depth:
                    is_last_at_depth = False
                    break
            
            # Build prefix with tree characters
            prefix_chars = ""
            for d in range(depth):
                # Check if there are more items at this ancestor depth
                has_more_at_d = False
                for future_item in tree_items[idx + 1:]:
                    if future_item['depth'] < d:
                        break
                    if future_item['depth'] == d:
                        has_more_at_d = True
                        break
                prefix_chars += "│   " if has_more_at_d else "    "
            
            # Add connector
            connector = "└── " if is_last_at_depth else "├── "
            
            if is_dir:
                display_name = f"{prefix_chars}{connector}[yellow]{name}/[/yellow]"
                table.add_row("", "", "", display_name)
            else:
                # Get type and comment for file
                file_type, comment = self._classify_iceberg_file(name, item['full_path'])
                
                # Color code by type
                type_styled = file_type
                if file_type == "Metadata":
                    type_styled = "[bold blue]Metadata[/bold blue]"
                elif file_type == "Manifest-List":
                    type_styled = "[bold cyan]Manifest-List[/bold cyan]"
                elif file_type == "Manifest":
                    type_styled = "[cyan]Manifest[/cyan]"
                elif file_type == "Data File":
                    type_styled = "[green]Data File[/green]"
                elif file_type == "Delete File":
                    type_styled = "[bold red]Delete File[/bold red]"
                
                display_name = f"{prefix_chars}{connector}[cyan]{name}[/cyan]"
                
                table.add_row(
                    self._format_size(item['file_info']['size']),
                    type_styled,
                    f"[dim]{comment}[/dim]",
                    display_name
                )
        
        console.print(table)
        console.print(f"[dim]Total: {len(files)} files[/dim]")


def create_explorer(
    minio_host: str = DEFAULT_MINIO_HOST,
    access_key: str = DEFAULT_ACCESS_KEY,
    secret_key: str = DEFAULT_SECRET_KEY
) -> CatalogExplorer:
    """Create a CatalogExplorer instance with default settings."""
    return CatalogExplorer(
        minio_host=minio_host,
        access_key=access_key,
        secret_key=secret_key
    )
