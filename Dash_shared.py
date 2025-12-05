# shared.py
import os
from pathlib import Path
import pandas as pd
from functools import lru_cache
from dash import Dash
from typing import Optional

app = Dash(__name__, suppress_callback_exceptions=True)

# Global variable to store loaded data
loaded_data = None
_LOADED_FILE: Optional[Path] = None

# ---------- NEW HELPERS ----------
def _candidate_data_roots() -> list[Path]:
    return [
        Path("/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data"),
        Path("/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data"),
        Path(__file__).resolve().parents[1] / "data",   # repo_root/data
        Path.cwd() / "data",
    ]

def _month_dirs(base: Path) -> list[Path]:
    if not base.exists():
        return []
    clean = base / "clean"
    if not clean.exists():
        return []
    # dirs named like 2025_08
    dirs = [p for p in clean.iterdir() if p.is_dir() and p.name[:4].isdigit() and "_" in p.name]
    # sort newest first
    return sorted(dirs, key=lambda p: p.name, reverse=True)

def _find_file(pattern_prefix: str, tag: Optional[str] = None) -> Optional[Path]:
    """
    Finds a file starting with pattern_prefix in clean folders.
    tag format: 'YYYY_MM' (e.g., '2025_08'). If None, pick latest.
    """
    for root in _candidate_data_roots():
        if not root.exists():
            continue
        if tag:
            # exact month
            d = root / "clean" / tag
            if d.exists():
                # Try exact match first then prefix
                f = d / f"{pattern_prefix}_{tag}.xlsx"
                if f.exists(): return f
                f = d / f"{pattern_prefix}_{tag}.xlsm"
                if f.exists(): return f
        else:
            # latest available
            for d in _month_dirs(root):
                tag2 = d.name
                f = d / f"{pattern_prefix}_{tag2}.xlsx"
                if f.exists(): return f
                f = d / f"{pattern_prefix}_{tag2}.xlsm"
                if f.exists(): return f
    return None

def get_loaded_file() -> Optional[Path]:
    """Returns the Path of the loaded clean file (or None)."""
    return _LOADED_FILE

# Function to load data
@lru_cache(maxsize=1)
def load_data() -> dict[str, pd.DataFrame]:
    """
    Loads R_Resumo (all sheets) and other requested files:
    - R_Resumo_YYYY_MM.xlsx (Main)
    - Conc_Estoq_YYYY_MM.xlsx
    - Conc_CARReceber_YYYY_MM.xlsx
    - R_Estoq_fdm_YYYY_MM.xlsx
    
    Returns a dictionary of DataFrames.
    """
    tag = os.environ.get("DASH_CLEAN_TAG")  # e.g., '2025_08'
    
    # 1. Main File: R_Resumo
    resumo_file = _find_file("R_Resumo", tag)
    if not resumo_file:
        # Try fallback name if R_Resumo not found (legacy support)
        resumo_file = _find_file("Kon_Report", tag)

    data_dict = {}
    
    if resumo_file:
        global _LOADED_FILE
        _LOADED_FILE = resumo_file
        print(f"[Dash_shared] Loading Main: {resumo_file}")
        try:
            # Load all sheets
            dfs = pd.read_excel(resumo_file, sheet_name=None)
            data_dict.update(dfs)
        except Exception as e:
            print(f"[Dash_shared] Error loading R_Resumo: {e}")
    else:
        print("[Dash_shared] R_Resumo (or Kon_Report) not found.")

    # 2. Additional Files
    # We try to find them in the SAME folder as R_Resumo if possible, or just latest
    # If we found R_Resumo, we can infer the tag from its parent folder
    current_tag = tag
    if not current_tag and resumo_file:
        current_tag = resumo_file.parent.name # e.g. 2025_11
        
    aux_files = {
        "Conc_Estoq": "Conc_Estoq",
        "Conc_CAR": "Conc_CARReceber",
        "R_Estoq": "R_Estoq_fdm"
    }
    
    for key, prefix in aux_files.items():
        fpath = _find_file(prefix, current_tag)
        if fpath:
            print(f"[Dash_shared] Loading Aux: {fpath}")
            try:
                # For aux files, we might want specific sheets or all. 
                # Let's load all and prefix keys or just store as is?
                # User asked to "load Conc_Estoq", implying the file content.
                # If it has multiple sheets, maybe we store them as "Conc_Estoq - SheetName"?
                # Or just "Conc_Estoq" if it's single sheet?
                # Let's load all sheets and add them to data_dict with prefix if multiple, 
                # or just the key if it's the main expected sheet.
                
                aux_dfs = pd.read_excel(fpath, sheet_name=None)
                for sheet_name, df in aux_dfs.items():
                    # Avoid overwriting R_Resumo sheets if names collide
                    # Use "FileName - SheetName" convention for clarity
                    safe_key = f"{key} - {sheet_name}"
                    data_dict[safe_key] = df
            except Exception as e:
                print(f"[Dash_shared] Error loading {key}: {e}")

    # Normalize columns for known sheets
    for key, df in data_dict.items():
        if "DATE" in df.columns:
            df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
        if "EMPRESA" in df.columns:
            df["EMPRESA"] = df["EMPRESA"].astype(str).str.strip().str.upper()
        if "MP" in df.columns:
            df["MP"] = df["MP"].astype(str).str.strip().str.upper()

    return data_dict