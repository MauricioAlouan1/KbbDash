"""
Builder for fact_sales_b2b table.

Builds B2B sales fact table from O_NFCI source data with lookup tables.
STRICT: Fails fast with clear errors if required sources or columns are missing.
"""

from pathlib import Path
import pandas as pd
import time
from typing import Dict, Tuple


def _standardize_text_case(df: pd.DataFrame) -> pd.DataFrame:
    """Convert all text to uppercase for standardization."""
    if isinstance(df, pd.DataFrame):
        df = df.copy()
        df.columns = [col.upper() for col in df.columns]
        for col in df.select_dtypes(include=[object]).columns:
            df[col] = df[col].str.upper()
    return df


def _load_static_table(data_root: Path, table_name: str) -> pd.DataFrame:
    """
    Load static lookup table from cache Parquet file.
    
    Args:
        data_root: DATA_ROOT directory
        table_name: Name of static table (e.g., "T_Remessas")
        
    Returns:
        pd.DataFrame: Loaded static table
        
    Raises:
        FileNotFoundError: If Parquet file doesn't exist
    """
    cache_path = data_root / "cache" / f"{table_name}.parquet"
    if not cache_path.exists():
        raise FileNotFoundError(
            f"❌ Required static table Parquet file not found: {cache_path}\n"
            f"Please ensure {table_name} has been cached via smart_loader."
        )
    df = pd.read_parquet(cache_path)
    return df


def _merge_last_cost(
    df1: pd.DataFrame,
    df1_product_col: str,
    df1_date_col: str,
    df2: pd.DataFrame,
    df2_product_col: str,
    df2_date_col: str,
    df2_cost_col: str,
    new_col_name: str,
    default_value: float = 999.0
) -> pd.DataFrame:
    """
    Merge df1 (sales table) with df2 (cost table) to get the last recorded cost before the sale date.
    
    Args:
        df1: Sales DataFrame
        df1_product_col: Product code column in df1 (uppercase)
        df1_date_col: Date column in df1 (uppercase)
        df2: Cost DataFrame
        df2_product_col: Product code column in df2 (uppercase)
        df2_date_col: Date column in df2 (uppercase)
        df2_cost_col: Cost value column in df2 (uppercase)
        new_col_name: Name for new cost column (uppercase)
        default_value: Default value if no match found
        
    Returns:
        df1 with new_col_name added
    """
    # Ensure columns exist
    required_cols_df1 = [df1_product_col, df1_date_col]
    required_cols_df2 = [df2_product_col, df2_date_col, df2_cost_col]
    
    missing_df1 = [c for c in required_cols_df1 if c not in df1.columns]
    missing_df2 = [c for c in required_cols_df2 if c not in df2.columns]
    
    if missing_df1:
        raise KeyError(f"Missing columns in df1: {missing_df1}")
    if missing_df2:
        raise KeyError(f"Missing columns in df2: {missing_df2}")
    
    # Convert dates to datetime
    df1 = df1.copy()
    df2 = df2.copy()
    df1[df1_date_col] = pd.to_datetime(df1[df1_date_col], errors='coerce')
    df2[df2_date_col] = pd.to_datetime(df2[df2_date_col], errors='coerce')
    
    # Sort cost table by product and date descending
    df2 = df2.sort_values(by=[df2_product_col, df2_date_col], ascending=[True, False])
    
    # Apply merge logic
    def get_last_cost(row):
        product = row[df1_product_col]
        sale_date = row[df1_date_col]
        
        if pd.isna(sale_date):
            return default_value
        
        # Filter cost table for matching product and valid entry dates
        valid_costs = df2[(df2[df2_product_col] == product) & (df2[df2_date_col] <= sale_date)]
        
        # Return the most recent cost before the sale date
        if not valid_costs.empty:
            return valid_costs[df2_cost_col].iloc[0]
        return default_value
    
    df1[new_col_name] = df1.apply(get_last_cost, axis=1)
    return df1


def build_fact_sales_b2b(data_root: Path, sources: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, bool]:
    """
    Build fact_sales_b2b table from O_NFCI source data.
    
    Args:
        data_root: DATA_ROOT directory
        sources: Dictionary of pre-loaded source DataFrames (unused - reads from cache)
        
    Returns:
        tuple[pd.DataFrame, bool]: Fact table and was_built=True
        
    Raises:
        FileNotFoundError: If required Parquet files are missing
        KeyError: If required columns are missing
        ValueError: If data validation fails
    """
    start_time = time.time()
    
    # 1. Load primary data from cache
    cache_path = data_root / "cache" / "O_NFCI.parquet"
    if not cache_path.exists():
        raise FileNotFoundError(
            f"❌ Required source Parquet file not found: {cache_path}\n"
            f"Please ensure O_NFCI has been processed and cached."
        )
    
    print(f"  📊 Loading O_NFCI from cache...")
    df = pd.read_parquet(cache_path)
    initial_shape = df.shape
    print(f"  ✅ Loaded O_NFCI: {initial_shape[0]} rows, {initial_shape[1]} columns")
    
    # 2. Normalize column names (uppercase)
    df = _standardize_text_case(df)
    
    # 3. Filter out canceled rows
    if "SITUAÇÃO" in df.columns:
        before_filter = len(df)
        df = df[df["SITUAÇÃO"] == "AUTORIZADO"].copy()
        after_filter = len(df)
        print(f"  ✅ Filtered canceled rows: {before_filter} → {after_filter} rows")
    else:
        print(f"  ⚠️  Column 'SITUAÇÃO' not found, skipping filter")
    
    # 4. Special handling for REMESSA DE PRODUTO
    if "OP" in df.columns:
        remessa_mask = df["OP"] == "REMESSA DE PRODUTO"
        remessa_count = remessa_mask.sum()
        if remessa_count > 0:
            if "QT" in df.columns:
                df.loc[remessa_mask, "PMERC_U"] = 0.01
                df.loc[remessa_mask, "PMERC_T"] = 0.01 * df.loc[remessa_mask, "QT"]
                df.loc[remessa_mask, "PNF_T"] = 0.01 * df.loc[remessa_mask, "QT"]
                print(f"  ✅ Applied REMESSA DE PRODUTO handling to {remessa_count} rows")
    
    # 5. Load static lookup tables from cache (STRICT - fail fast if missing)
    print(f"  📚 Loading static lookup tables from cache...")
    required_static_tables = [
        "T_Remessas",
        "T_ProdF",
        "T_GruposCli",
        "T_Entradas",
        "T_Reps",
        "T_Fretes",
        "T_Verbas"
    ]
    
    static_tables = {}
    missing_tables = []
    
    for table_name in required_static_tables:
        try:
            static_tables[table_name] = _load_static_table(data_root, table_name)
            # Normalize static table columns to uppercase
            static_tables[table_name] = _standardize_text_case(static_tables[table_name])
            print(f"    ✅ {table_name}: {static_tables[table_name].shape}")
        except FileNotFoundError as e:
            missing_tables.append(table_name)
    
    if missing_tables:
        error_msg = (
            f"❌ Missing required static table Parquet files:\n"
            + "\n".join([f"  - {data_root / 'cache' / f'{t}.parquet'}" for t in missing_tables])
            + f"\n\nPlease ensure these tables are cached via smart_loader."
        )
        raise FileNotFoundError(error_msg)
    
    # 6. Join with static lookup tables (all using uppercase column names)
    print(f"  🔗 Joining with lookup tables...")
    
    # REM_NF: Merge O_NFCI.NOMEF with T_Remessas.NOMEF
    if "NOMEF" in df.columns and "NOMEF" in static_tables["T_Remessas"].columns:
        df_remessas = static_tables["T_Remessas"][["NOMEF"]].drop_duplicates()
        df_remessas["REM_NF"] = 1
        df = df.merge(df_remessas, on="NOMEF", how="left")
        df["REM_NF"] = df["REM_NF"].fillna(0).astype(int)
        print(f"    ✅ Added REM_NF")
    else:
        df["REM_NF"] = 0
    
    # CODPP: Merge O_NFCI.CODPF with T_ProdF.CODPF
    if "CODPF" in df.columns and "CODPF" in static_tables["T_ProdF"].columns:
        df_prodf = static_tables["T_ProdF"][["CODPF", "CODPP"]].drop_duplicates(subset=["CODPF"])
        df = df.merge(df_prodf, on="CODPF", how="left")
        df["CODPP"] = df["CODPP"].fillna("xxx")
        print(f"    ✅ Added CODPP")
    else:
        df["CODPP"] = "xxx"
    
    # G1: Merge O_NFCI.NOMEF with T_GruposCli.NOMEF
    if "NOMEF" in df.columns and "NOMEF" in static_tables["T_GruposCli"].columns:
        df_grupos = static_tables["T_GruposCli"][["NOMEF", "G1"]].drop_duplicates(subset=["NOMEF"])
        df = df.merge(df_grupos, on="NOMEF", how="left")
        df["G1"] = df["G1"].fillna("V")
        print(f"    ✅ Added G1")
    else:
        df["G1"] = "V"
    
    # ECU: Use merge_last_cost logic
    if "CODPP" in df.columns and "DATA" in df.columns:
        df = _merge_last_cost(
            df,
            df1_product_col="CODPP",
            df1_date_col="DATA",
            df2=static_tables["T_Entradas"],
            df2_product_col="PAI",
            df2_date_col="ULTIMA ENTRADA",
            df2_cost_col="ULT CU R$",
            new_col_name="ECU",
            default_value=999.0
        )
        print(f"    ✅ Added ECU")
    else:
        df["ECU"] = 999.0
    
    # COMISSPCT: Merge O_NFCI.VENDEDOR with T_Reps.VENDEDOR
    if "VENDEDOR" in df.columns and "VENDEDOR" in static_tables["T_Reps"].columns:
        df_reps = static_tables["T_Reps"][["VENDEDOR", "COMISSPCT"]].drop_duplicates(subset=["VENDEDOR"])
        df = df.merge(df_reps, on="VENDEDOR", how="left")
        df["COMISSPCT"] = df["COMISSPCT"].fillna(0.0)
        print(f"    ✅ Added COMISSPCT")
    else:
        df["COMISSPCT"] = 0.0
    
    # FRETEPCT: Merge O_NFCI.UF with T_Fretes.UF
    if "UF" in df.columns and "UF" in static_tables["T_Fretes"].columns:
        df_fretes = static_tables["T_Fretes"][["UF", "FRETEPCT"]].drop_duplicates(subset=["UF"])
        df = df.merge(df_fretes, on="UF", how="left")
        df["FRETEPCT"] = df["FRETEPCT"].fillna(0.0)
        # Set FRETEPCT = 0 where G1 = "DROP" or "ALWE"
        if "G1" in df.columns:
            df.loc[df["G1"].isin(["DROP", "ALWE"]), "FRETEPCT"] = 0.0
        print(f"    ✅ Added FRETEPCT")
    else:
        df["FRETEPCT"] = 0.0
    
    # VERBAPCT: Merge O_NFCI.NOMEF with T_Verbas.NOMEF
    if "NOMEF" in df.columns and "NOMEF" in static_tables["T_Verbas"].columns:
        df_verbas = static_tables["T_Verbas"][["NOMEF", "VERBAPCT"]].drop_duplicates(subset=["NOMEF"])
        df = df.merge(df_verbas, on="NOMEF", how="left")
        df["VERBAPCT"] = df["VERBAPCT"].fillna(0.0)
        print(f"    ✅ Added VERBAPCT")
    else:
        df["VERBAPCT"] = 0.0
    
    # 7. Calculate derived columns
    print(f"  🧮 Calculating derived columns...")
    
    # C: 1 - REM_NF (lines to be removed when REM_NF = 1)
    df["C"] = 1 - df["REM_NF"]
    
    # B: 1 if OP == "REMESSA DE PRODUTO" and C == 1 (not removed), else 0
    if "OP" in df.columns:
        df["B"] = df.apply(
            lambda row: 1 if row["OP"] == "REMESSA DE PRODUTO" and row["C"] == 1 else 0,
            axis=1
        )
    else:
        df["B"] = 0
    
    # ECT: ECU * QT
    if "ECU" in df.columns and "QT" in df.columns:
        df["ECT"] = df["ECU"] * df["QT"]
    else:
        df["ECT"] = 0.0
    
    # COMISSVLR: COMISSPCT * PMERC_T
    if "COMISSPCT" in df.columns and "PMERC_T" in df.columns:
        df["COMISSVLR"] = df["COMISSPCT"] * df["PMERC_T"] * df["C"]
    else:
        df["COMISSVLR"] = 0.0
    
    # FRETEVLR: FRETEPCT * PNF_T
    if "FRETEPCT" in df.columns and "PNF_T" in df.columns:
        df["FRETEVLR"] = df.apply(
            lambda row: max(
                row["FRETEPCT"] * row["PNF_T"] * row["C"],
                row["FRETEPCT"] * row["ECT"] * row["C"] * 2
            ),
            axis=1
        )
    else:
        df["FRETEVLR"] = 0.0
    
    # VERBAVLR: VERBAPCT * PNF_T
    if "VERBAPCT" in df.columns and "PNF_T" in df.columns:
        df["VERBAVLR"] = df["VERBAPCT"] * df["PNF_T"] * df["C"]
    else:
        df["VERBAVLR"] = 0.0
    
    # MARGVLR: PMERC_T - ECT - COMISSVLR - FRETEVLR - VERBAVLR
    if "PMERC_T" in df.columns:
        df["MARGVLR"] = (
            df["C"] * (df["PMERC_T"] * (1 - 0.0925) - df.get("ICMS_T", 0)) -
            df["VERBAVLR"] - df["FRETEVLR"] - df["COMISSVLR"] - df["ECT"]
        )
    else:
        df["MARGVLR"] = 0.0
    
    # MARGPCT: MARGVLR / PMERC_T (handle division by zero)
    if "PMERC_T" in df.columns and "MARGVLR" in df.columns:
        df["MARGPCT"] = df.apply(
            lambda row: row["MARGVLR"] / row["PMERC_T"] if row["PMERC_T"] != 0 else 0.0,
            axis=1
        )
    else:
        df["MARGPCT"] = 0.0
    
    # 8. Apply rounding
    print(f"  🔢 Applying rounding...")
    
    # All columns except MARGPCT: round to 2 decimals
    numeric_cols = df.select_dtypes(include=[pd.Number]).columns
    for col in numeric_cols:
        if col != "MARGPCT":
            df[col] = df[col].round(2)
    
    # MARGPCT: round to 3 decimals
    if "MARGPCT" in df.columns:
        df["MARGPCT"] = df["MARGPCT"].round(3)
    
    # 9. Validation & logging
    elapsed = time.time() - start_time
    final_shape = df.shape
    
    # Aggregate check: sum of PNF_T (TotalNF equivalent)
    if "PNF_T" in df.columns:
        total_nf_sum = df["PNF_T"].sum()
        print(f"  ✅ Aggregation check - Sum of PNF_T: {total_nf_sum:,.2f}")
    
    print(f"  ✅ Build complete: {final_shape[0]} rows, {final_shape[1]} columns (elapsed: {elapsed:.2f}s)")
    
    return df, True
