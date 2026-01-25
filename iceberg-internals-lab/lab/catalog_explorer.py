"""
AIStor Catalog Explorer

Helper functions to explore the Iceberg catalog metadata and data files
stored in MinIO AIStor.
"""

import json
import requests
import boto3
from typing import List, Dict, Any, Optional
from tabulate import tabulate
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.tree import Tree

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
    
    def print_snapshots(self, metadata: Dict):
        """Print snapshot history from table metadata."""
        snapshots = metadata.get("metadata", {}).get("snapshots", [])
        
        if not snapshots:
            console.print("[yellow]No snapshots found[/yellow]")
            return
        
        table_data = []
        for snap in snapshots:
            table_data.append([
                snap.get("snapshot-id", ""),
                snap.get("timestamp-ms", ""),
                snap.get("summary", {}).get("operation", ""),
                snap.get("summary", {}).get("added-data-files", "0"),
                snap.get("summary", {}).get("deleted-data-files", "0"),
                snap.get("summary", {}).get("added-records", "0"),
                snap.get("summary", {}).get("deleted-records", "0"),
            ])
        
        headers = ["Snapshot ID", "Timestamp", "Operation", "Added Files", "Deleted Files", "Added Rows", "Deleted Rows"]
        console.print(tabulate(table_data, headers=headers, tablefmt="simple"))
    
    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"


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
