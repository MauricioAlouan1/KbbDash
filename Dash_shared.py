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

def _find_conc_file(tag: str | None = None) -> Path | None:
    """
    tag format: 'YYYY_MM' (e.g., '2025_08'). If None, pick latest.
    """
    for root in _candidate_data_roots():
        if not root.exists():
            continue
        if tag:
            # exact month
            d = root / "clean" / tag
            if d.exists():
                f = d / f"Conc_Estoq_{tag}.xlsx"
                if f.exists():
                    return f
        else:
            # latest available
            for d in _month_dirs(root):
                tag2 = d.name
                f = d / f"Conc_Estoq_{tag2}.xlsx"
                if f.exists():
                    return f
    return None

def get_loaded_file() -> Optional[Path]:
    """Returns the Path of the loaded clean file (or None)."""
    return _LOADED_FILE

# Function to load data
@lru_cache(maxsize=1)
def load_data() -> pd.DataFrame:
    """
    Loads the latest (or specified) clean reconciliation file:
    data/clean/YYYY_MM/Conc_Estoq_YYYY_MM.xlsx

    Optional override:
      export DASH_CLEAN_TAG=2025_08
    """
    tag = os.environ.get("DASH_CLEAN_TAG")  # e.g., '2025_08'
    conc_file = _find_conc_file(tag)

    if conc_file is None:
        # Return an empty frame but keep expected shape-friendly behavior
        print("[Dash_shared] No clean file found in any data roots.")
        return pd.DataFrame()

    # --- inside load_data(), after you resolve conc_file ---
    global _LOADED_FILE
    _LOADED_FILE = conc_file

    print(f"[Dash_shared] Loading: {conc_file}")
    try:
        df = pd.read_excel(conc_file, sheet_name="Child")  # primary sheet
    except Exception:
        # fallback to first sheet if name differs
        df = pd.read_excel(conc_file)

    # Normalize a couple of common columns if present
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    if "EMPRESA" in df.columns:
        df["EMPRESA"] = df["EMPRESA"].astype(str).str.strip().str.upper()
    if "MP" in df.columns:
        df["MP"] = df["MP"].astype(str).str.strip().str.upper()

    return df