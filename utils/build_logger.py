"""
Build logger for tracking semantic model rebuild operations.

Appends build records to CSV log file.
"""

from pathlib import Path
import csv
from datetime import datetime
from typing import Optional


def log_build(
    table_name: str,
    status: str,
    rows: int = 0,
    seconds: float = 0.0,
    data_root: Path = None,
) -> None:
    """
    Log a build operation to the build log CSV.
    
    Args:
        table_name: Name of the table that was built
        status: Status string (e.g., "rebuilt", "skipped", "error")
        rows: Number of rows in the table
        seconds: Elapsed time in seconds
        data_root: DATA_ROOT directory (optional, will be resolved if None)
    """
    if data_root is None:
        from config.paths import get_data_root
        data_root = get_data_root()
    
    meta_dir = data_root / "_meta"
    meta_dir.mkdir(exist_ok=True)
    
    log_path = meta_dir / "_build_log.csv"
    
    # Check if file exists and has headers
    file_exists = log_path.exists()
    
    # Append mode
    with open(log_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        # Write header if new file
        if not file_exists:
            writer.writerow(["timestamp", "table_name", "status", "rows", "elapsed_seconds"])
        
        # Write record
        timestamp = datetime.now().isoformat()
        writer.writerow([timestamp, table_name, status, rows, f"{seconds:.2f}"])

