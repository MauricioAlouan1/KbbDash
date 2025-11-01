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
    
    # 3. Process each source and track loaded sources
    changed_sources = []
    loaded_sources = {}  # Track all loaded sources for builders
    
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
                
                # Store loaded source for builders
                loaded_sources[source_name] = df
                
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
    
    # 5b. Load all sources needed by fact tables to rebuild
    # Collect all required sources from dependencies
    required_sources = set()
    for fact_name in facts_to_rebuild:
        if fact_name in dependencies:
            required_sources.update(dependencies[fact_name])
    
    # Load any missing required sources
    for source_name in required_sources:
        if source_name not in loaded_sources and source_name in sources_map:
            try:
                excel_files = resolve_source_files(sources_map[source_name], data_root)
                if excel_files:
                    df, _ = load_excel_if_changed(source_name, excel_files, data_root)
                    loaded_sources[source_name] = df
            except Exception as e:
                print(f"⚠️  Warning: Could not load required source {source_name}: {e}")
    
    # 6. Rebuild fact tables using builders
    facts_dir = data_root / "facts"
    facts_dir.mkdir(exist_ok=True)
    
    for fact_name in facts_to_rebuild:
        print(f"🔨 Rebuilding {fact_name}...", end=" ", flush=True)
        start_time = time.time()
        
        # Try to find and call builder module
        builder_name = f"build_fact_{fact_name}"
        try:
            # Dynamic import of builder module
            builder_module = __import__(f"builders.{builder_name}", fromlist=[builder_name])
            builder_func = getattr(builder_module, builder_name)
            
            # Call builder with loaded sources
            df = builder_func(data_root, loaded_sources)
            
        except ImportError as e:
            # Builder module doesn't exist - raise clear error (STRICT)
            raise ImportError(
                f"❌ Builder module 'builders.{builder_name}' not found for fact table '{fact_name}'.\n"
                f"Error: {e}\n"
                f"Create builders/{builder_name}.py with a {builder_name}() function."
            )
        except AttributeError as e:
            # Builder function doesn't exist - raise clear error (STRICT)
            raise AttributeError(
                f"❌ Builder function '{builder_name}' not found in builders.{builder_name}.\n"
                f"Error: {e}\n"
                f"Ensure the module exports a function named {builder_name}(data_root, sources)."
            )
        except Exception as e:
            # Re-raise builder errors with context (STRICT - let errors propagate)
            raise RuntimeError(
                f"❌ Error building {fact_name} using builders.{builder_name}: {e}"
            ) from e
        
        # Save as Parquet
        parquet_path = facts_dir / f"{fact_name}.parquet"
        df.to_parquet(parquet_path, index=False)
        
        elapsed = time.time() - start_time
        
        # Log build
        rows = len(df) if not df.empty else 0
        log_build(fact_name, "rebuilt", rows=rows, seconds=elapsed, data_root=data_root)
        
        print(f"✅ ({rows} rows)")
    
    print(f"\n✅ Rebuild complete: {len(facts_to_rebuild)} fact table(s) processed")
    return 0


if __name__ == "__main__":
    exit(main())

