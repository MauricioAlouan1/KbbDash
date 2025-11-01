#!/usr/bin/env python3
"""
Semantic Model Builder - Main Orchestrator

Builds incremental semantic model from Excel sources with smart caching
and dependency-aware rebuilds.
"""

import json
import glob
from pathlib import Path
import pandas as pd
import time

from config.paths import get_data_root
from utils.smart_loader import load_excel_if_changed
from utils.dependency_graph import get_all_dependents
from utils.build_logger import log_build


def load_config(config_path: Path) -> dict:
    """Load JSON config file."""
    if not config_path.exists():
        raise FileNotFoundError(f"❌ Config file not found: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def expand_source_pattern(pattern: str, data_root: Path) -> list[Path]:
    """
    Expand a glob pattern relative to DATA_ROOT.
    
    Returns list of matching file paths.
    """
    full_pattern = data_root / pattern
    matches = glob.glob(str(full_pattern))
    return [Path(m) for m in matches]


def expand_source_path(path: str, data_root: Path) -> Path:
    """
    Expand a fixed path relative to DATA_ROOT.
    
    Returns single Path.
    """
    return data_root / path


def resolve_source_files(source_config: dict, data_root: Path) -> list[Path]:
    """
    Resolve source files from config (pattern or path).
    
    Returns list of Path objects.
    """
    if "pattern" in source_config:
        return expand_source_pattern(source_config["pattern"], data_root)
    elif "path" in source_config:
        path = expand_source_path(source_config["path"], data_root)
        return [path] if path.exists() else []
    else:
        raise ValueError(f"Source config must have 'pattern' or 'path': {source_config}")


def main():
    """Main orchestrator function."""
    print("🚀 Starting Semantic Model Builder\n")
    
    # 1. Resolve DATA_ROOT
    try:
        data_root = get_data_root()
        print(f"✅ DATA_ROOT: {data_root}\n")
    except FileNotFoundError as e:
        print(str(e))
        return 1
    
    # 2. Load configs from repo
    repo_root = Path(__file__).parent.resolve()
    sources_map_path = repo_root / "config" / "sources_map.json"
    dependencies_path = repo_root / "config" / "model_dependencies.json"
    
    try:
        sources_map = load_config(sources_map_path)
        dependencies = load_config(dependencies_path)
        print(f"✅ Loaded sources map: {len(sources_map)} sources")
        print(f"✅ Loaded dependencies: {len(dependencies)} fact tables\n")
    except FileNotFoundError as e:
        print(str(e))
        return 1
    
    # 3. Process each source
    changed_sources = []
    
    print("📋 Processing sources...")
    for source_name, source_config in sources_map.items():
        try:
            excel_files = resolve_source_files(source_config, data_root)
            
            if not excel_files:
                print(f"⚠️  {source_name}: No matching files found (skipping)")
                continue
            
            # Load with smart caching
            try:
                df, was_reloaded = load_excel_if_changed(source_name, excel_files, data_root)
                
                # Track if source changed
                if was_reloaded:
                    changed_sources.append(source_name)
                    print(f"   → {source_name} changed")
                
            except Exception as e:
                print(f"❌ Error loading {source_name}: {e}")
                continue
                
        except Exception as e:
            print(f"❌ Error processing {source_name}: {e}")
            continue
    
    print()
    
    # 4. Check if any changes
    if not changed_sources:
        print("✅ All sources up-to-date. Nothing to rebuild.")
        return 0
    
    print(f"🔄 Changed sources: {', '.join(changed_sources)}\n")
    
    # 5. Compute rebuild set
    facts_to_rebuild = get_all_dependents(changed_sources, dependencies)
    
    if not facts_to_rebuild:
        print("ℹ️  No fact tables depend on changed sources. Nothing to rebuild.")
        return 0
    
    print(f"🔨 Fact tables to rebuild: {', '.join(facts_to_rebuild)}\n")
    
    # 6. Rebuild fact tables (simulate for now)
    facts_dir = data_root / "facts"
    facts_dir.mkdir(exist_ok=True)
    
    for fact_name in facts_to_rebuild:
        print(f"🔨 Rebuilding {fact_name}...", end=" ", flush=True)
        start_time = time.time()
        
        # Create empty placeholder DataFrame
        df = pd.DataFrame()
        
        # Save as Parquet
        parquet_path = facts_dir / f"{fact_name}.parquet"
        df.to_parquet(parquet_path, index=False)
        
        elapsed = time.time() - start_time
        time.sleep(0.5)  # Simulate processing time
        elapsed += 0.5
        
        # Log build
        log_build(fact_name, "rebuilt", rows=0, seconds=elapsed, data_root=data_root)
        
        print("✅")
    
    print(f"\n✅ Rebuild complete: {len(facts_to_rebuild)} fact table(s) processed")
    return 0


if __name__ == "__main__":
    exit(main())

