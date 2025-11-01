"""
Smart Excel loader with Parquet caching.

Loads Excel files only when they have changed, otherwise uses cached Parquet.
Tracks modification times in metadata JSON file.
"""

from pathlib import Path
import json
import pandas as pd
from typing import Optional, Tuple
import time


def _get_metadata_path(data_root: Path) -> Path:
    """Get path to the metadata JSON file."""
    meta_dir = data_root / "_meta"
    meta_dir.mkdir(exist_ok=True)
    return meta_dir / "_last_loaded.json"


def _load_metadata(data_root: Path) -> dict:
    """Load metadata JSON, returning empty dict if missing."""
    metadata_path = _get_metadata_path(data_root)
    if not metadata_path.exists():
        return {}
    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def _save_metadata(data_root: Path, metadata: dict) -> None:
    """Save metadata JSON."""
    metadata_path = _get_metadata_path(data_root)
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)


def _get_cache_path(data_root: Path, source_name: str) -> Path:
    """Get path to cached Parquet file."""
    cache_dir = data_root / "cache"
    cache_dir.mkdir(exist_ok=True)
    return cache_dir / f"{source_name}.parquet"


def _check_files_exist(excel_files: list[Path]) -> tuple[bool, list[Path]]:
    """Check which Excel files exist. Returns (all_exist, existing_files)."""
    existing = [f for f in excel_files if f.exists()]
    return len(existing) == len(excel_files), existing


def load_excel_if_changed(source_name: str, excel_files: list[Path], data_root: Path) -> Tuple[pd.DataFrame, bool]:
    """
    Load Excel file(s) only if changed, otherwise use cached Parquet.
    
    Args:
        source_name: Logical name of the source (for cache naming)
        excel_files: List of Excel file paths to load
        data_root: DATA_ROOT directory
        
    Returns:
        tuple[pd.DataFrame, bool]: Loaded data (from cache or Excel) and whether it was reloaded
        
    Raises:
        FileNotFoundError: If no Excel files found and no cache exists
    """
    if not excel_files:
        raise FileNotFoundError(f"❌ No Excel files provided for source: {source_name}")
    
    metadata = _load_metadata(data_root)
    cache_path = _get_cache_path(data_root, source_name)
    
    # Check if all Excel files exist
    all_exist, existing_files = _check_files_exist(excel_files)
    
    if not all_exist and cache_path.exists():
        # Parquet exists but some Excel files are missing
        missing = [f for f in excel_files if not f.exists()]
        print(f"⚠️  Parquet cache exists but source Excel file(s) missing:")
        for f in missing:
            print(f"   - {f}")
        response = input("Delete stale Parquet cache? (y/n): ").strip().lower()
        if response == "y":
            cache_path.unlink()
            if source_name in metadata:
                del metadata[source_name]
            _save_metadata(data_root, metadata)
            print(f"✅ Deleted stale cache for {source_name}")
        else:
            # Load from cache if it exists
            if cache_path.exists():
                print(f"📊 {source_name}: Loading from cache (missing Excel files ignored)")
                return pd.read_parquet(cache_path), False
            else:
                raise FileNotFoundError(
                    f"❌ No cache and missing Excel files for {source_name}. Cannot proceed."
                )
    
    if not all_exist:
        raise FileNotFoundError(
            f"❌ Some Excel files missing for {source_name}. "
            f"Missing: {[f for f in excel_files if not f.exists()]}"
        )
    
    # Get current modification times
    current_mtimes = {str(f): f.stat().st_mtime for f in excel_files}
    
    # Check if cached
    cached_info = metadata.get(source_name, {})
    cached_mtimes = cached_info.get("mtimes", {})
    
    # Compare modification times
    needs_reload = False
    if cached_mtimes != current_mtimes:
        needs_reload = True
    elif not cache_path.exists():
        needs_reload = True
    
    if needs_reload:
        # Load from Excel
        print(f"📂 Loading {source_name} from Excel...")
        start_time = time.time()
        
        dfs = []
        for excel_file in excel_files:
            try:
                df = pd.read_excel(excel_file)
                dfs.append(df)
            except Exception as e:
                raise IOError(f"❌ Error reading {excel_file}: {e}")
        
        # Concatenate if multiple files
        if len(dfs) > 1:
            df = pd.concat(dfs, ignore_index=True)
        else:
            df = dfs[0]
        
        # Save to cache
        cache_path.parent.mkdir(exist_ok=True)
        df.to_parquet(cache_path, index=False)
        
        # Update metadata
        metadata[source_name] = {
            "mtimes": current_mtimes,
            "parquet_path": str(cache_path),
            "file_count": len(excel_files),
        }
        _save_metadata(data_root, metadata)
        
        elapsed = time.time() - start_time
        rows, cols = df.shape
        print(f"📊 {source_name}: {rows} rows, {cols} cols [loaded in {elapsed:.2f}s]")
        return df, True
    else:
        # Load from cache
        print(f"📊 {source_name}: Loading from cache...")
        df = pd.read_parquet(cache_path)
        rows, cols = df.shape
        print(f"📊 {source_name}: {rows} rows, {cols} cols [cached]")
        return df, False

