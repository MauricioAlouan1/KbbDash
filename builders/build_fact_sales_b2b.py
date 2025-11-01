"""
Builder for fact_sales_b2b table.

Builds B2B sales fact table from O_NFCI source data with optional lookup tables.
STRICT: Fails fast with clear errors if required sources or columns are missing.
"""

from pathlib import Path
import pandas as pd
from typing import Dict


def build_fact_sales_b2b(data_root: Path, sources: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Build fact_sales_b2b table from O_NFCI source data.
    
    Args:
        data_root: DATA_ROOT directory (for any path resolution needed)
        sources: Dictionary of pre-loaded source DataFrames {source_name: DataFrame}
        
    Returns:
        pd.DataFrame: Fact table ready to save as Parquet
        
    Raises:
        FileNotFoundError: If required sources are missing from sources dict
        KeyError: If required columns are missing from source DataFrames
        ValueError: If data validation fails (empty DataFrame, wrong types, etc.)
    """
    # STRICT: Validate required sources exist
    required_sources = ["O_NFCI"]
    missing_sources = [s for s in required_sources if s not in sources]
    if missing_sources:
        raise FileNotFoundError(
            f"❌ Required sources missing for fact_sales_b2b: {missing_sources}\n"
            f"Available sources: {list(sources.keys())}"
        )
    
    # Load primary source
    df = sources["O_NFCI"].copy()
    
    # STRICT: Validate primary source is not empty
    if df.empty:
        raise ValueError("❌ O_NFCI source DataFrame is empty. Cannot build fact_sales_b2b.")
    
    # STRICT: Validate required columns exist
    required_columns = ["CODPF", "Qt", "Data", "AnoMes"]
    missing_columns = [c for c in required_columns if c not in df.columns]
    if missing_columns:
        raise KeyError(
            f"❌ Required columns missing in O_NFCI: {missing_columns}\n"
            f"Available columns: {list(df.columns)}"
        )
    
    # Optional: Merge with lookup tables if available
    # T_CondPagto and T_Reps are optional - only merge if present
    if "T_CondPagto" in sources:
        # Example merge (adjust based on actual column names and merge keys)
        # df = df.merge(sources["T_CondPagto"], left_on="...", right_on="...", how="left")
        pass
    
    if "T_Reps" in sources:
        # Example merge (adjust based on actual column names and merge keys)
        # df = df.merge(sources["T_Reps"], left_on="...", right_on="...", how="left")
        pass
    
    # For now, return the source data with minimal transformation
    # Later: Add full merge logic and transformations as needed
    
    return df

