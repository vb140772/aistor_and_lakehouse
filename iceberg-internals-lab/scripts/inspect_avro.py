#!/usr/bin/env python3
"""
Utility script to inspect Avro files on macOS.
Can read from file path or stdin (for use with mc cat, etc.)
"""

import sys
import json
import argparse
from pathlib import Path
import fastavro
import io
from rich.console import Console
from rich.syntax import Syntax
from rich.panel import Panel
from rich.table import Table

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

def inspect_avro_file(file_path=None, stdin_data=None, show_records=False, max_records=5, output_format='yaml'):
    """Inspect an Avro file and display schema and metadata."""
    
    console = Console()
    
    # Read file content
    if stdin_data:
        content = stdin_data
    elif file_path:
        with open(file_path, 'rb') as f:
            content = f.read()
    else:
        console.print("[red]Error: No file path or stdin data provided[/red]", file=sys.stderr)
        return 1
    
    if not content:
        console.print("[red]Error: Empty file[/red]", file=sys.stderr)
        return 1
    
    try:
        # Open Avro file
        reader = fastavro.reader(io.BytesIO(content))
        schema = reader.schema
        
        # Count records
        record_count = 0
        sample_records = []
        
        for record in reader:
            record_count += 1
            if show_records and len(sample_records) < max_records:
                sample_records.append(record)
        
        # Display header
        console.print("\n")
        console.print(Panel.fit(
            "[bold cyan]AVRO FILE INSPECTION[/bold cyan]",
            border_style="cyan"
        ))
        
        # File info table
        info_table = Table(show_header=False, box=None, padding=(0, 2))
        info_table.add_column(style="bold", width=12)
        info_table.add_column()
        
        if file_path:
            info_table.add_row("[bold]File:[/bold]", file_path)
        else:
            info_table.add_row("[bold]File:[/bold]", "[dim](from stdin)[/dim]")
        
        info_table.add_row("[bold]Size:[/bold]", f"{len(content):,} bytes")
        info_table.add_row("[bold]Records:[/bold]", f"{record_count:,}")
        
        console.print("\n")
        console.print(info_table)
        
        # Schema with syntax highlighting
        console.print("\n")
        
        # Convert to YAML or JSON based on format preference
        if output_format == 'yaml' and YAML_AVAILABLE:
            try:
                schema_output = yaml.dump(schema, default_flow_style=False, sort_keys=False, allow_unicode=True)
                syntax_lang = "yaml"
            except Exception:
                # Fallback to JSON if YAML conversion fails
                schema_output = json.dumps(schema, indent=2)
                syntax_lang = "json"
        else:
            schema_output = json.dumps(schema, indent=2)
            syntax_lang = "json"
        
        console.print("[bold]Schema:[/bold]")
        console.print(Syntax(schema_output, syntax_lang, theme="monokai", padding=1, line_numbers=True))
        
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
        
        console.print("\n")
        return 0
        
    except Exception as e:
        console.print(f"[red]Error reading Avro file: {e}[/red]", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


def main():
    parser = argparse.ArgumentParser(
        description="Inspect Avro files - display schema and metadata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Read from file (default: YAML format, more compact)
  python inspect_avro.py file.avro
  
  # Read from stdin (with mc cat)
  mc cat local/iceberglab/path/file.avro | python inspect_avro.py
  
  # Show sample records
  python inspect_avro.py file.avro --show-records
  
  # Use JSON format instead of YAML
  python inspect_avro.py file.avro --format json
  
  # Show more records
  python inspect_avro.py file.avro --show-records --max-records 10
        """
    )
    
    parser.add_argument(
        'file',
        nargs='?',
        help='Path to Avro file (or use stdin)'
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
    
    # Warn if YAML requested but not available
    if args.format == 'yaml' and not YAML_AVAILABLE:
        console = Console()
        console.print("[yellow]Warning: PyYAML not installed. Falling back to JSON format.[/yellow]", file=sys.stderr)
        console.print("[dim]Install with: pip install pyyaml[/dim]", file=sys.stderr)
        args.format = 'json'
    
    # Check if we have stdin data
    stdin_data = None
    if not sys.stdin.isatty():
        stdin_data = sys.stdin.buffer.read()
    
    # If we have both file and stdin, prefer file
    if args.file and stdin_data:
        console = Console()
        console.print("[yellow]Warning: Both file path and stdin provided. Using file path.[/yellow]", file=sys.stderr)
        stdin_data = None
    
    return inspect_avro_file(
        file_path=args.file,
        stdin_data=stdin_data,
        show_records=args.show_records,
        max_records=args.max_records,
        output_format=args.format
    )


if __name__ == '__main__':
    sys.exit(main())
