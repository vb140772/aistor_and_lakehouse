#!/usr/bin/env python3
"""
Utility script to inspect Parquet files on macOS.
Can read from file path or stdin (for use with mc cat, etc.)
"""

import sys
import json
import argparse
from pathlib import Path
import io
from rich.console import Console
from rich.syntax import Syntax
from rich.panel import Panel
from rich.table import Table

try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

def inspect_parquet_file(file_path=None, stdin_data=None, show_records=False, max_records=5, output_format='yaml'):
    """Inspect a Parquet file and display schema and metadata."""
    
    console = Console()
    console_err = Console(file=sys.stderr)
    
    if not PARQUET_AVAILABLE:
        console_err.print("[red]Error: pyarrow is not installed[/red]")
        console_err.print("[dim]Install with: pip install pyarrow[/dim]")
        return 1
    
    # Read file content
    if stdin_data:
        content = stdin_data
        # PyArrow needs a file-like object, so we'll use BytesIO
        parquet_file = io.BytesIO(content)
    elif file_path:
        parquet_file = file_path
    else:
        console_err = Console(file=sys.stderr)
        console_err.print("[red]Error: No file path or stdin data provided[/red]")
        return 1
    
    try:
        # Open Parquet file
        parquet_file_obj = pq.ParquetFile(parquet_file)
        metadata = parquet_file_obj.metadata
        schema = parquet_file_obj.schema_arrow
        
        # Get file size
        if file_path:
            file_size = Path(file_path).stat().st_size
        else:
            file_size = len(stdin_data) if stdin_data else 0
        
        # Read sample records if requested
        sample_records = []
        if show_records:
            try:
                table = parquet_file_obj.read_row_groups([0], columns=None)  # Read first row group
                df = table.to_pandas()
                
                # Limit to max_records
                sample_df = df.head(max_records)
                sample_records = sample_df.to_dict('records')
            except Exception as e:
                console.print(f"[yellow]Warning: Could not read sample records: {e}[/yellow]")
        
        # Display header
        console.print("\n")
        console.print(Panel.fit(
            "[bold cyan]PARQUET FILE INSPECTION[/bold cyan]",
            border_style="cyan"
        ))
        
        # File info table
        info_table = Table(show_header=False, box=None, padding=(0, 2))
        info_table.add_column(style="bold", width=16)
        info_table.add_column()
        
        if file_path:
            info_table.add_row("[bold]File:[/bold]", file_path)
        else:
            info_table.add_row("[bold]File:[/bold]", "[dim](from stdin)[/dim]")
        
        info_table.add_row("[bold]Size:[/bold]", f"{file_size:,} bytes")
        info_table.add_row("[bold]Rows:[/bold]", f"{metadata.num_rows:,}")
        info_table.add_row("[bold]Row Groups:[/bold]", str(metadata.num_row_groups))
        info_table.add_row("[bold]Columns:[/bold]", str(len(schema)))
        info_table.add_row("[bold]Created By:[/bold]", metadata.created_by or "[dim]unknown[/dim]")
        
        console.print("\n")
        console.print(info_table)
        
        # Schema information
        console.print("\n")
        console.print("[bold]Schema:[/bold]")
        
        schema_info = []
        for i, field in enumerate(schema):
            field_info = {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable
            }
            if field.metadata:
                field_info["metadata"] = {k.decode() if isinstance(k, bytes) else k: 
                                         (v.decode() if isinstance(v, bytes) else v) 
                                         for k, v in field.metadata.items()}
            schema_info.append(field_info)
        
        # Convert to YAML or JSON
        if output_format == 'yaml' and YAML_AVAILABLE:
            try:
                schema_output = yaml.dump(schema_info, default_flow_style=False, sort_keys=False, allow_unicode=True)
                syntax_lang = "yaml"
            except Exception:
                schema_output = json.dumps(schema_info, indent=2)
                syntax_lang = "json"
        else:
            schema_output = json.dumps(schema_info, indent=2)
            syntax_lang = "json"
        
        console.print(Syntax(schema_output, syntax_lang, theme="monokai", padding=1, line_numbers=True))
        
        # Row group information
        console.print("\n")
        console.print("[bold]Row Groups:[/bold]")
        
        row_groups_table = Table(show_header=True, header_style="bold")
        row_groups_table.add_column("Group", justify="right")
        row_groups_table.add_column("Rows", justify="right")
        row_groups_table.add_column("Size (bytes)", justify="right")
        row_groups_table.add_column("Compression", width=12)
        
        for i in range(metadata.num_row_groups):
            row_group = metadata.row_group(i)
            # Get compression from first column if available
            compression = "[dim]N/A[/dim]"
            if row_group.num_columns > 0:
                try:
                    first_col = row_group.column(0)
                    compression = first_col.compression or "[dim]N/A[/dim]"
                except:
                    pass
            
            row_groups_table.add_row(
                str(i),
                f"{row_group.num_rows:,}",
                f"{row_group.total_byte_size:,}",
                compression
            )
        
        console.print(row_groups_table)
        
        # Column statistics (if available)
        if metadata.num_row_groups > 0:
            first_row_group = metadata.row_group(0)
            if first_row_group.num_columns > 0:
                # Check if first column has statistics
                try:
                    first_col = first_row_group.column(0)
                    if first_col.statistics:
                        console.print("\n")
                        console.print("[bold]Column Statistics (first row group):[/bold]")
                        
                        stats_table = Table(show_header=True, header_style="bold")
                        stats_table.add_column("Column")
                        stats_table.add_column("Min", justify="right")
                        stats_table.add_column("Max", justify="right")
                        stats_table.add_column("Nulls", justify="right")
                        
                        for col_idx in range(first_row_group.num_columns):
                            col = first_row_group.column(col_idx)
                            stats = col.statistics
                            if stats:
                                stats_table.add_row(
                                    col.path_in_schema,
                                    str(stats.min) if stats.has_min else "[dim]N/A[/dim]",
                                    str(stats.max) if stats.has_max else "[dim]N/A[/dim]",
                                    f"{stats.null_count:,}" if stats.has_null_count else "[dim]N/A[/dim]"
                                )
                        
                        console.print(stats_table)
                except Exception:
                    pass  # Skip statistics if not available
        
        # Sample records with syntax highlighting
        if show_records and sample_records:
            console.print("\n")
            console.print(f"[bold]Sample Records (first {len(sample_records)}):[/bold]")
            
            for i, record in enumerate(sample_records, 1):
                # Convert to YAML or JSON
                if output_format == 'yaml' and YAML_AVAILABLE:
                    try:
                        record_output = yaml.dump(record, default_flow_style=False, sort_keys=False, allow_unicode=True, default=str)
                        record_syntax_lang = "yaml"
                    except Exception:
                        record_output = json.dumps(record, indent=2, default=str)
                        record_syntax_lang = "json"
                else:
                    record_output = json.dumps(record, indent=2, default=str)
                    record_syntax_lang = "json"
                
                console.print(f"\n[bold yellow]Record {i}:[/bold yellow]")
                console.print(Syntax(record_output, record_syntax_lang, theme="monokai", padding=1, line_numbers=False))
        
        # File metadata
        if metadata.metadata:
            console.print("\n")
            console.print("[bold]File Metadata:[/bold]")
            
            metadata_dict = {k.decode() if isinstance(k, bytes) else k: 
                           (v.decode() if isinstance(v, bytes) else v) 
                           for k, v in metadata.metadata.items()}
            
            if output_format == 'yaml' and YAML_AVAILABLE:
                try:
                    meta_output = yaml.dump(metadata_dict, default_flow_style=False, sort_keys=False, allow_unicode=True)
                    meta_syntax_lang = "yaml"
                except Exception:
                    meta_output = json.dumps(metadata_dict, indent=2)
                    meta_syntax_lang = "json"
            else:
                meta_output = json.dumps(metadata_dict, indent=2)
                meta_syntax_lang = "json"
            
            console.print(Syntax(meta_output, meta_syntax_lang, theme="monokai", padding=1, line_numbers=False))
        
        console.print("\n")
        return 0
        
    except Exception as e:
        console_err = Console(file=sys.stderr)
        console_err.print(f"[red]Error reading Parquet file: {e}[/red]")
        import traceback
        traceback.print_exc()
        return 1


def main():
    parser = argparse.ArgumentParser(
        description="Inspect Parquet files - display schema and metadata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Read from file (default: YAML format, more compact)
  python inspect_parquet.py file.parquet
  
  # Read from stdin (with mc cat)
  mc cat local/iceberglab/path/file.parquet | python inspect_parquet.py
  
  # Show sample records
  python inspect_parquet.py file.parquet --show-records
  
  # Use JSON format instead of YAML
  python inspect_parquet.py file.parquet --format json
  
  # Show more records
  python inspect_parquet.py file.parquet --show-records --max-records 10
        """
    )
    
    parser.add_argument(
        'file',
        nargs='?',
        help='Path to Parquet file (or use stdin)'
    )
    
    parser.add_argument(
        '--show-records',
        action='store_true',
        help='Display sample records'
    )
    
    parser.add_argument(
        '--max-records',
        type=int,
        default=5,
        help='Maximum number of sample records to show (default: 5)'
    )
    
    parser.add_argument(
        '--format',
        choices=['yaml', 'json'],
        default='yaml',
        help='Output format: yaml (default, more compact) or json'
    )
    
    args = parser.parse_args()
    
    # Check if we have stdin data
    stdin_data = None
    if not sys.stdin.isatty():
        stdin_data = sys.stdin.buffer.read()
    
    # If we have both file and stdin, prefer file
    if args.file and stdin_data:
        console_err = Console(file=sys.stderr)
        console_err.print("[yellow]Warning: Both file path and stdin provided. Using file path.[/yellow]")
        stdin_data = None
    
    # Warn if YAML requested but not available
    if args.format == 'yaml' and not YAML_AVAILABLE:
        console_err = Console(file=sys.stderr)
        console_err.print("[yellow]Warning: PyYAML not installed. Falling back to JSON format.[/yellow]")
        console_err.print("[dim]Install with: pip install pyyaml[/dim]")
        args.format = 'json'
    
    return inspect_parquet_file(
        file_path=args.file,
        stdin_data=stdin_data,
        show_records=args.show_records,
        max_records=args.max_records,
        output_format=args.format
    )


if __name__ == '__main__':
    sys.exit(main())
