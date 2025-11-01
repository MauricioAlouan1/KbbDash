"""
Builder for fact_sales_b2c table.

Builds B2C sales fact table from L_LPI source data.
STRICT: Fails fast with clear errors if required sources or columns are missing.
"""

from pathlib import Path
import pandas as pd
from typing import Dict


def build_fact_sales_b2c(data_root: Path, sources: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Build fact_sales_b2c table from L_LPI source data.
    
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
    required_sources = ["L_LPI"]
    missing_sources = [s for s in required_sources if s not in sources]
    if missing_sources:
        raise FileNotFoundError(
            f"❌ Required sources missing for fact_sales_b2c: {missing_sources}\n"
            f"Available sources: {list(sources.keys())}"
        )
    
    # Load primary source
    df = sources["L_LPI"].copy()
    
    # STRICT: Validate primary source is not empty
    if df.empty:
        raise ValueError("❌ L_LPI source DataFrame is empty. Cannot build fact_sales_b2c.")
    
    # STRICT: Validate required columns exist
    required_columns = ["CODPF", "Qt", "Data", "PMerc_T", "PMerc_U"]
    missing_columns = [c for c in required_columns if c not in df.columns]
    if missing_columns:
        raise KeyError(
            f"❌ Required columns missing in L_LPI: {missing_columns}\n"
            f"Available columns: {list(df.columns)}"
        )
    
    # For now, return the source data with minimal transformation
    # Later: Add full merge logic and transformations as needed
    
    return df

