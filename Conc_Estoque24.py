#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Inventory reconciliation for a given Year/Month.

Inputs (auto-resolved from 2 candidate base folders):
- Previous month inventory summary: R_Estoque_fdm_YYYY_MM.xlsx (sheet PT01)
- Current  month inventory summary: R_Estoque_fdm_YYYY_MM.xlsx (sheet PT01)
- Monthly sales summary:            R_Resumo_YYYY_MM.xlsx (sheets O_NFCI, L_LPI)
- Arrivals (table):                 Tables/T_Entradas.xlsx  (all months; we filter by the target month)

Output:
CSV + Excel with columns:
CODPF, QT_I, CU_I, CT_I, VENDAS_2b, VENDAS_2c, Qt_E, CU_E, CT_E, Qt_SS, CU_F, CT_Ger
"""

from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
import numpy as np
from typing import List, Optional, Tuple
from itertools import cycle

# -----------------------
# CONFIG – adjust these!
# -----------------------
# Two possible base folders, like your process_inv.py pattern
path_options = [
    '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/',
    '/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data'
]
for candidate in path_options:
    if os.path.exists(candidate):
        base_dir = candidate
        break
else:
    print("None of the specified directories exist.")
    base_dir = None

# Subfolder with Tables
TABLES_SUBDIR = "Tables"
INPUT_SUBDIR = "clean"
OUTPUT_SUBDIR = "clean"

# <<< HARD-CODE AQUI: nomes dos arquivos (prefixos) e tabela de Entradas
INV_PREFIX      = "R_Estoq_fdm_"     # gera "R_Estoque_fdm_YYYY_MM.xlsx"
RESUMO_PREFIX   = "R_Resumo_"          # gera "R_Resumo_YYYY_MM.xlsx"
ENTRADAS_FILE   = "T_Entradas.xlsx"    # dentro de TABLES_DIR

# <<< HARD-CODE AQUI: nomes das ABAS
SHEET_PT01   = "PT01"
SHEET_ONFCI  = "O_NFCI"
SHEET_LLPI   = "L_LPI"

# <<< HARD-CODE AQUI: nomes das COLUNAS por aba/tabela
# PT01 (estoque)

# O_NFCI (vendas 2b)
ONFCI_CODE_COL = "CODPF"               # ex.: "CODPF" ou "Código do Produto"
ONFCI_QTY_COL  = "QTD"                  # ex.: "QT", "Quantidade", "QTD"

# L_LPI (vendas 2c)
LLPI_CODE_COL    = "CODPP"
LLPI_QTY_COL     = "QTD"
LLPI_STATUS_COL  = "STATUS PEDIDO"     # usado para filtrar != "CANCELADO"
LLPI_EMPRESA_COL = "EMPRESA"           # usado para filtrar == "K"

# T_Entradas (entradas do mês)
ENTR_CODE_COL   = "Filho"              # ex.: "CODPF", "Pai"
ENTRP_CODE_COL   = "Pai"              # ex.: "CODPF", "Pai"
ENTR_QTY_COL    = "Qt_E"                 # ex.: "QT", "Qtde"
ENTR_CU_COL     = "CU_E"                 # ex.: "CU" (se não tiver, use None)
ENTR_CT_COL     = "CT_E"                 # ex.: "CT" (se não tiver, use None e calcula QT*CU)
# Filtragem por mês:
ENTR_ANOMES_COL = "AnoMes"             # yymm (ex.: 2507). Se não existir, use None.
ENTR_DATE_COL   = None                 # OU nome da data (ex.: "Emissao" ou "Ultima Entrada") se preferir filtrar por data

CLEAN_ROOT = os.path.join(base_dir, INPUT_SUBDIR) 
TABLES_DIR = os.path.join(base_dir, TABLES_SUBDIR)

# -----------------------
# Helpers
# -----------------------
def find_existing_excel(base_path: Path, base_name: str) -> Path:
    """
    Tenta encontrar arquivo com base em base_name + .xlsx ou .xlsm
    """
    for ext in [".xlsx", ".xlsm"]:
        candidate = base_path / f"{base_name}{ext}"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Arquivo não encontrado: {base_path}/{base_name}.xlsx ou .xlsm")

def ym_to_prev(year: int, month: int) -> Tuple[int, int]:
    if month == 1:
        return (year - 1, 12)
    return (year, month - 1)

def yymm_to_str(year: int, month: int) -> str:
    return f"{year:04d}_{month:02d}"

def find_existing_file(candidates: List[Path]) -> Optional[Path]:
    for p in candidates:
        if p.exists():
            return p
    return None

def resolve_month_dir(year: int, month: int) -> Path:
    tag = f"{year:04d}_{month:02d}"
    p = Path(os.path.join(CLEAN_ROOT, tag))
    if not p.exists():
        raise FileNotFoundError(f"Pasta do mês não encontrada: {p}")
    return p

def resolve_tables_dir(year: int, month: int) -> Path:
    p = Path(os.path.join(TABLES_DIR))
    if not p.exists():
        raise FileNotFoundError(f"Pasta das Tabelas não encontrada: {p}")
    return p

def read_excel_safe(path: Path, sheet_name: Optional[str] = None) -> pd.DataFrame:
    try:
        return pd.read_excel(path, sheet_name=sheet_name, dtype=str) if sheet_name else pd.read_excel(path, dtype=str)
    except ValueError:
        # some files contain mixed dtypes; retry without dtype enforcement
        return pd.read_excel(path, sheet_name=sheet_name)

def norm_code(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.upper()

def first_existing_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in df.columns}
    for name in candidates:
        if name in df.columns:
            return name
        if name.lower() in lower_map:
            return lower_map[name.lower()]
    return None

def coerce_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0)

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

# -----------------------
# Loaders / Normalizers
# -----------------------

def load_prodf(tables_dir: Path) -> pd.DataFrame:
    path = tables_dir / "T_Prodf.xlsx"
    if not path.exists():
        raise FileNotFoundError(f"Arquivo Prodf.xlsx não encontrado em {tables_dir}")
    df = pd.read_excel(path)
    df["CODPF"] = df["CodPF"].astype(str).str.strip().str.upper()
    df["CODPP"] = df["CodPP"].astype(str).str.strip().str.upper()
    return df[["CODPF", "CODPP"]]

def load_curr_inventory_data(file_path: Path) -> pd.DataFrame:
    df = pd.read_excel(file_path, sheet_name="Data")

    # <<< HARD-CODE — usa exatamente as colunas definidas no topo
    code_col = "Pai"
    df["CODPP"] = df[code_col]
    df["Qt_SS"] = df["Quantidade_Inv"]

    # Consolida por item
    agg = df.groupby("CODPP", as_index=False).agg({
        # summed values
        "Qt_SS": "sum",
        "CT_F": "sum",

        # repeated values → use average (or 'first' if more reliable)
        "CU_E": "mean",
        "CU_S": "mean",
        "CU_F": "mean",
        "PGE": "mean",
        "Qt_E": "mean"
    })
    return agg

def load_prev_inventory_data(file_path: Path) -> pd.DataFrame:
    print(f"🟡 Loading previous inventory from: {file_path}")
    df = pd.read_excel(file_path, sheet_name="PT_pp")

    # Identify the code column
    code_col = "Pai"
    if code_col not in df.columns:
        print("❌ Column 'Pai' not found — please check file header names.")
    else:
        print(f"✅ Using code column: {code_col}")

    # Map your expected columns
    for src_col, dst_col in [("Qt_Ger", "Qt_I"), ("CT_Ger", "CT_I"), ("CU_F", "CU_I")]:
        if src_col not in df.columns:
            print(f"⚠️ Missing expected column '{src_col}' in PT_pp.")
            df[dst_col] = 0
        else:
            df[dst_col] = pd.to_numeric(df[src_col], errors="coerce").fillna(0)

    # Copy product code
    df["CODPP"] = df[code_col].astype(str).str.strip().str.upper()

    # Keep only wanted cols
    interest_cols = ["CODPP", "Qt_I", "CT_I", "CU_I"]
    dfkeep = df[interest_cols].drop_duplicates("CODPP", keep="first")

    return dfkeep

def load_sales_onfci(resumo_path: Path) -> pd.DataFrame:
    df = pd.read_excel(resumo_path, sheet_name="O_NFCI")

    CODPP = df["CODPP"].astype(str).str.strip().str.upper()
    QT    = pd.to_numeric(df["QTD"], errors="coerce").fillna(0)
    VV    = pd.to_numeric(df["MERCVLR"], errors="coerce").fillna(0)   # sales value
    Mrg   = pd.to_numeric(df["MARGVLR"], errors="coerce").fillna(0)   # contribution margin

    out = pd.DataFrame({
        "CODPP": CODPP,
        "VENDAS_2b": QT,
        "VV_2b": VV,
        "Mrg_2b": Mrg
    })

    onfci_out = out.groupby("CODPP", as_index=False).agg({
        "VENDAS_2b":"sum",
        "VV_2b":"sum",
        "Mrg_2b":"sum"
    })

    return onfci_out

def load_sales_llpi(resumo_path: Path) -> pd.DataFrame:
    df = pd.read_excel(resumo_path, sheet_name=SHEET_LLPI)

    # Filtros fixos
    df = df[df[LLPI_STATUS_COL].astype(str).str.upper() != "CANCELADO"]
    df = df[df[LLPI_EMPRESA_COL].astype(str).str.upper() == "K"]

    CODPP = df[LLPI_CODE_COL].astype(str).str.strip().str.upper()
    QT    = pd.to_numeric(df[LLPI_QTY_COL], errors="coerce").fillna(0)
    VV    = pd.to_numeric(df["VLRVENDA"], errors="coerce").fillna(0)
    Mrg   = pd.to_numeric(df["MargVlr"], errors="coerce").fillna(0)

    out = pd.DataFrame({
        "CODPP": CODPP,
        "VENDAS_2c": QT,
        "VV_2c": VV,
        "Mrg_2c": Mrg
    })
    lpi_out = out.groupby("CODPP", as_index=False).agg({
        "VENDAS_2c":"sum",
        "VV_2c":"sum",
        "Mrg_2c":"sum"
    })

    return lpi_out


# -----------------------
# Main reconciliation
# -----------------------

def reconcile_inventory(year: int, month: int) -> pd.DataFrame:
    import numpy as np
    import pandas as pd
    from pathlib import Path

    # --- helpers ---
    def norm_code(s: pd.Series) -> pd.Series:
        return s.astype(str).str.strip().str.upper()

    # prev / this tags
    prev_y = year if month > 1 else year - 1
    prev_m = month - 1 if month > 1 else 12
    this_tag = f"{year:04d}_{month:02d}"
    prev_tag = f"{prev_y:04d}_{prev_m:02d}"

    # dirs
    this_dir = resolve_month_dir(year, month)
    prev_dir = resolve_month_dir(prev_y, prev_m)
    tables_dir = resolve_tables_dir(year, month)

    # files
    prev_inv_path = find_existing_excel(prev_dir, f"{INV_PREFIX}{prev_tag}")
    this_inv_path = find_existing_excel(this_dir,  f"{INV_PREFIX}{this_tag}")
    resumo_path   = find_existing_excel(this_dir,  f"{RESUMO_PREFIX}{this_tag}")

    for p in [prev_inv_path, this_inv_path, resumo_path]:
        if not p.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {p}")

    # --- load base data ---
    inv_prev = load_prev_inventory_data(prev_inv_path)    # expects CODPP, Qt_I, CU_I, CT_I
    inv_this = load_curr_inventory_data(this_inv_path)    # expects CODPP, Qt_SS, CU_F, CT_Ger (or CT_SS)
    vendas_b = load_sales_onfci(resumo_path)              # CODPP, VENDAS_2b, VV_2b, Mrg_2b
    vendas_c = load_sales_llpi(resumo_path)               # CODPP, VENDAS_2c, VV_2c, Mrg_2c
    prodf    = load_prodf(tables_dir)                     # CODPP, CODPF (structure)

    # --- normalize keys ---
    for df_ in (inv_prev, inv_this, vendas_b, vendas_c, prodf):
        if "CODPP" in df_.columns:
            df_["CODPP"] = norm_code(df_["CODPP"])
        if "CODPF" in df_.columns:
            df_["CODPF"] = norm_code(df_["CODPF"])

    # --- Identify codes that appear in the structure table (product parts) ---
    prodf_parts = prodf["CODPP"].drop_duplicates()
    # --- Flag items NOT present in prodf as 'I' (independent / not a part) ---
    inv_this["Ins"] = np.where(~inv_this["CODPP"].isin(prodf_parts), "I", None)

    # ⚠️ CRITICAL: avoid many-to-many merges.
    # Do NOT merge prodf into vendas_* or inv_prev. Keep sales aggregated by CODPP only.
    vendas_b = vendas_b.groupby("CODPP", as_index=False).agg({
        "VENDAS_2b": "sum", "VV_2b": "sum", "Mrg_2b": "sum"
    })
    vendas_c = vendas_c.groupby("CODPP", as_index=False).agg({
        "VENDAS_2c": "sum", "VV_2c": "sum", "Mrg_2c": "sum"
    })

    # Build child-level df starting from inv_this (one row per CODPP ideally)
    df = inv_this.copy()

    # If CU_S is not present, fallback to CU_F or CU_I
    if "CU_S" not in df.columns:
        df["CU_S"] = df.get("CU_F", pd.Series(np.nan, index=df.index)).fillna(df.get("CU_I", 0))

    # merge sales and previous inventory (all on CODPP only)
    df = df.merge(vendas_b, on="CODPP", how="left")
    df = df.merge(vendas_c, on="CODPP", how="left")
    df = df.merge(inv_prev[["CODPP", "Qt_I", "CT_I", "CU_I"]], on="CODPP", how="left")

    # numeric fills
    for c in ["Qt_I","CT_I","CU_I","Qt_SS","CU_F","CT_Ger",
              "VENDAS_2b","VENDAS_2c","VV_2b","VV_2c","Mrg_2b","Mrg_2c",
              "Qt_E","CU_E","CT_E"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)


    # since df should already be 1 row per CODPP, this .groupby is defensive
    agg_map = {}
    for col in df.columns:
        if col == "CODPP":
            continue
        # keep the first for flags/identifiers/cost baselines; sum for numeric flows
        if col in ["Ins","CU_I","CU_E","CU_S","CU_F"]:
            agg_map[col] = "first"
        elif pd.api.types.is_numeric_dtype(df[col]):
            agg_map[col] = "sum"
        else:
            agg_map[col] = "first"

    dp = df.groupby("CODPP", as_index=False).agg(agg_map)

    # sales quantity from channels
    dp["VENDAS_tot"] = dp.get("VENDAS_2b", 0) + dp.get("VENDAS_2c", 0)
    dp["Qt_S"]       = dp["VENDAS_tot"]

    # values & margins
    dp["VV_tot"]   = dp.get("VV_2b", 0) + dp.get("VV_2c", 0)
    dp["Mrg_tot"]  = dp.get("Mrg_2b", 0) + dp.get("Mrg_2c", 0)

    vv2b   = dp.get("VV_2b", pd.Series(0, index=dp.index))
    vv2c   = dp.get("VV_2c", pd.Series(0, index=dp.index))
    vvtot  = dp.get("VV_tot", pd.Series(0, index=dp.index))
    mrg2b  = dp.get("Mrg_2b", pd.Series(0, index=dp.index))
    mrg2c  = dp.get("Mrg_2c", pd.Series(0, index=dp.index))
    mrgtot = dp.get("Mrg_tot", pd.Series(0, index=dp.index))

    dp["MrgPct_2b"]  = np.where(vv2b != 0, mrg2b / vv2b, 0)
    dp["MrgPct_2c"]  = np.where(vv2c != 0, mrg2c / vv2c, 0)
    dp["MrgPct_tot"] = np.where(vvtot != 0, mrgtot / vvtot, 0)
    for c in ["MrgPct_2b","MrgPct_2c","MrgPct_tot"]:
        dp[c] = dp[c].apply(lambda x: max(x, -1))

    # Stock math
    dp["Qt_E"]   = dp.get("Qt_E", 0)
    dp["Qt_SS"]  = dp.get("Qt_SS", 0)
    dp["Qt_I"]   = dp.get("Qt_I", 0)
    dp["Qt_SE"]  = dp["Qt_I"] + dp["Qt_E"] - dp["Qt_S"]
    dp["Qt_Diff"] = dp["Qt_SS"] - dp["Qt_SE"]
    dp["Qt_Ger"]  = np.where(dp["Qt_Diff"] == 0, dp["Qt_SS"], np.nan)

    # Costs
    dp["CU_E"]  = dp.get("CU_E", 0)
    dp["CU_F"]  = dp.get("CU_F", 0)
    dp["CU_S"]  = dp.get("CU_S", dp["CU_F"])
    dp["CT_S"]  = (dp["CU_S"] * dp["Qt_S"]).round(2)
    dp["CT_E"]  = (dp["CU_E"] * dp["Qt_E"]).round(2)
    dp["CT_SE"] = (dp.get("CT_I", 0) + dp["CT_E"] - dp["CT_S"]).round(2)
    dp["CT_SS"] = (dp["Qt_SS"] * dp["CU_F"]).round(2)
    dp["CT_Diff"] = (dp["CT_SS"] - dp["CT_SE"]).round(2)
    dp["CT_Ger"]  = np.where(dp["Qt_Diff"] == 0, dp["CT_SS"], np.nan)

    # Unit cost diffs & derived CT diff from quantity
    dp["CU_Diff"]    = np.where(dp["Qt_SS"] > 0, dp["CU_F"] - dp.get("CU_I", 0), 0).round(2)

    dp["AnoMes"] = (year - 2000) * 100 + month

    # Ordering
    dp["CODPP"] = norm_code(dp["CODPP"])
    dp = dp.sort_values("CODPP", kind="stable").reset_index(drop=True)

    final_cols_order = [
        "CODPP", "Ins",
        "Qt_I", "Qt_E", "Qt_S", "Qt_SE", "Qt_SS", "Qt_Diff", "Qt_Ger",
        "CT_I", "CT_E", "CT_S", "CT_SE", "CT_SS", "CT_Diff", "CT_Ger",
        "CU_I", "CU_E", "CU_S", "CU_F",
        "VENDAS_2b", "VENDAS_2c", "VENDAS_tot",
        "VV_2b", "VV_2c", "VV_tot",
        "Mrg_2b", "Mrg_2c", "Mrg_tot",
        "MrgPct_2b", "MrgPct_2c", "MrgPct_tot",
        "CU_Diff", "AnoMes"
    ]
    # create VENDAS_tot for output if needed
    dp["VENDAS_tot"] = dp["VENDAS_2b"].fillna(0) + dp["VENDAS_2c"].fillna(0)

    dp = dp[[c for c in final_cols_order if c in dp.columns]]
    return dp

def apply_excel_formatting(ws, df, wb):
    """Apply column widths and styles to an Excel worksheet."""
    col_widths = {
        "CODPP": 10,
        "Ins": 2,

        # Quantities
        "Qt_I": 6, "Qt_E": 5, "Qt_S": 5, "Qt_SE": 6, "Qt_SS": 6,
        "Qt_Diff": 5, "Qt_Ger": 6,

        # Adjustments
        "Qt_Aj": 5, "Qt_AjF": 5, "CT_Aj": 9, "CT_AjF": 9,

        # Costs
        "CT_I": 10, "CT_E": 10, "CT_S": 10, "CT_SE": 10,
        "CT_SS": 10, "CT_Diff": 10, "CT_Ger": 10,

        # Unit costs
        "CU_I": 6, "CU_E": 6, "CU_S": 6, "CU_F": 6,

        # Sales and margins
        "VV_2b": 10, "VV_2c": 10, "VV_tot": 10,
        "Mrg_2b": 10, "Mrg_2c": 10, "Mrg_tot": 10,
        "MrgPct_2b": 6, "MrgPct_2c": 6, "MrgPct_tot": 6,

        # Vendas
        "VENDAS_2b": 9, "VENDAS_2c": 9, "VENDAS_tot": 9,
    }

    # --- Base formats ---
    qt_blue   = wb.add_format({'num_format': '#,##0',   'bg_color': '#DDEBF7'})
    qt_gray   = wb.add_format({'num_format': '#,##0',   'bg_color': "#E3EEF7"})
    ct_orange = wb.add_format({'num_format': '#,##0.00','bg_color': "#D7A167"})
    ct_gray   = wb.add_format({'num_format': '#,##0.00','bg_color': "#E3EEF7"})
    cu_green  = wb.add_format({'num_format': '#,##0.00','bg_color': "#8ADBEA"})

    # --- Additional formats ---
    blue_money_fmt   = wb.add_format({'num_format': '#,##0.00', 'bg_color': '#DDEBF7'})
    green_money_fmt  = wb.add_format({'num_format': '#,##0.00', 'bg_color': '#E2EFDA'})
    blue_pct_fmt = wb.add_format({'num_format': '0%', 'bg_color': '#DDEBF7'})
    orange_money_fmt = wb.add_format({'num_format': '#,##0.00', 'bg_color': '#FCE4D6'})
    lightblue_int_fmt = wb.add_format({'num_format': '#,##0', 'bg_color': '#BDD7EE'})  # for Qt_Aj / Qt_AjF
    lightblue_money   = wb.add_format({'num_format': '#,##0.00', 'bg_color': '#BDD7EE'})  # for CT_Aj / CT_AjF

    # --- Column groups ---
    qt_blue_cols  = {"Qt_I","Qt_E","Qt_S","Qt_SE","Qt_SS"}
    qt_gray_cols  = {"Qt_Diff","Qt_Ger"}
    ct_orange_cols= {"CT_I","CT_E","CT_S","CT_SE","CT_SS"}
    ct_gray_cols  = {"CT_Diff","CT_Ger"}
    cu_green_cols = {"CU_I","CU_E","CU_S","CU_F"}
    blue_cols     = {"VV_2b","VV_2c","VV_tot"}  # VV_tot same style
    green_cols    = {"Mrg_2b","Mrg_2c","Mrg_tot"}
    pct_cols      = {"MrgPct_2b","MrgPct_2c","MrgPct_tot"}
    orange_cols   = {"VENDAS_2b","VENDAS_2c","VENDAS_tot"}
    aj_int_cols   = {"Qt_Aj","Qt_AjF"}
    aj_money_cols = {"CT_Aj","CT_AjF"}

    # --- Apply ---
    ws.autofilter(0, 0, df.shape[0], df.shape[1] - 1)
    for idx, col in enumerate(df.columns):
        width = col_widths.get(col, 12)
        fmt = None
        if   col in qt_gray_cols:   fmt = qt_gray
        elif col in qt_blue_cols:   fmt = qt_blue
        elif col in ct_orange_cols: fmt = ct_orange
        elif col in ct_gray_cols:   fmt = ct_gray
        elif col in cu_green_cols:  fmt = cu_green
        elif col in blue_cols:      fmt = blue_money_fmt
        elif col in green_cols:     fmt = green_money_fmt
        elif col in pct_cols:       fmt = blue_pct_fmt
        elif col in orange_cols:    fmt = orange_money_fmt
        elif col in aj_int_cols:    fmt = lightblue_int_fmt
        elif col in aj_money_cols:  fmt = lightblue_money

        ws.set_column(idx, idx, width, fmt)


def adjust_missing_inventory_progressive(dp):
    """
    Adjusts missing/excess inventory progressively within ±2% of CT_S total.
    Step 1: Fully offsets the smaller of the two sides (positive vs. negative Qt_Diff),
            removing neutralized items from further consideration.
    Step 2: Adds the value of the neutralized side to the available budget.
    Step 3: Uses that budget to adjust the remaining side progressively,
            starting with the cheapest items first.
    """

    import numpy as np

    # --- Step 1. Setup and budget ---
    total_ct_s = dp["CT_S"].sum(skipna=True)
    budget_limit = total_ct_s * 0.02
    print(f"💰 Total cost of goods sold (CT_S): {total_ct_s:,.2f}")
    print(f"🎯 Budget range: ±{budget_limit:,.2f}")

    # Pending rows = not reconciled and not already 'Ins'
    mask_pending = (dp["CT_Ger"].isna() | dp["Qt_Ger"].isna()) & (dp["Ins"] != "I")
    pending = dp.loc[mask_pending].copy()
    pending = pending.dropna(subset=["CU_F", "Qt_Diff"])
    pending["Qt_Aj"] = 0.0
    pending["CT_DiffVal"] = pending["Qt_Diff"] * pending["CU_F"]

    # --- Step 2. Separate sides ---
    pos = pending[pending["Qt_Diff"] > 0].copy()
    neg = pending[pending["Qt_Diff"] < 0].copy()
    total_pos = (pos["Qt_Diff"] * pos["CU_F"]).sum()
    total_neg = (neg["Qt_Diff"] * neg["CU_F"]).sum()  # will be negative
    print(f"➕ Positive side total: {total_pos:,.2f}")
    print(f"➖ Negative side total: {total_neg:,.2f}")

    # --- Step 3. Determine smaller side and neutralize it completely ---
    smaller_side = "pos" if abs(total_pos) <= abs(total_neg) else "neg"
    offset_value = min(abs(total_pos), abs(total_neg))
    print(f"⚖️  Natural offset applied (no budget impact): {offset_value:,.2f}")
    print(f"🧭 Smaller side: {smaller_side}")

    # Fully neutralize smaller side
    if smaller_side == "pos":
        pending.loc[pos.index, "Qt_Aj"] = pending.loc[pos.index, "Qt_Diff"]
    else:
        pending.loc[neg.index, "Qt_Aj"] = pending.loc[neg.index, "Qt_Diff"]

    # Remove those items from further adjustment
    if smaller_side == "pos":
        remaining = pending.loc[neg.index].copy()
    else:
        remaining = pending.loc[pos.index].copy()

    # --- Step 4. Add offset value to the usable budget ---
    remaining_budget = budget_limit + offset_value
    print(f"💵 Budget available for adjustment after neutralization: {remaining_budget:,.2f}")

    # --- Step 5. Adjust remaining side progressively (round-robin, cheapest first) ---
    from itertools import cycle

    remaining = remaining.sort_values("CU_F", ascending=True).copy()
    total_used = 0.0

    # Initialize helper columns if missing
    if "Qt_Aj" not in remaining.columns:
        remaining["Qt_Aj"] = 0.0

    # Create cycling iterator over item indices
    active = remaining.index.tolist()
    iter_cycle = cycle(active)

    while active and total_used < remaining_budget:
        i = next(iter_cycle)
        if i not in active:
            continue

        unit_cost = remaining.at[i, "CU_F"]
        diff = remaining.at[i, "Qt_Diff"]
        adj  = remaining.at[i, "Qt_Aj"]
        direction = np.sign(diff)

        # Still has room to adjust and budget left?
        if abs(adj) < abs(diff) and total_used + unit_cost <= remaining_budget:
            remaining.at[i, "Qt_Aj"] += direction
            total_used += unit_cost
        else:
            # This item is fully adjusted or over budget—remove it from cycle
            active.remove(i)
            if not active:
                break
            iter_cycle = cycle(active)
    # --- Sync back round-robin results into pending ---
    pending.loc[remaining.index, "Qt_Aj"] = remaining["Qt_Aj"].astype(float)

    print(f"✅ Budget used for remaining side: {total_used:,.2f}")
    print(f"📊 Final budget usage: {total_used / remaining_budget * 100:+.1f}%")

    # --- Step 6. Compute costs and merge back ---
    pending["CT_Aj"]  = pending["Qt_Aj"] * pending["CU_F"]
    pending["Qt_AjF"] = pending["Qt_Diff"] - pending["Qt_Aj"]
    pending["CT_AjF"] = pending["Qt_AjF"] * pending["CU_F"]


    # --- Merge back into dp ---
    for col in ["Qt_Aj", "CT_Aj", "Qt_AjF", "CT_AjF"]:
        if col not in dp.columns:
            dp.loc[:, col] = 0.0
        dp.loc[pending.index, col] = pending[col].values

    # --- Final reconciled balances (uniform, avoids notna pitfalls) ---
    dp.loc[:, "Qt_Ger"] = dp["Qt_SE"] + dp["Qt_Aj"]
    dp.loc[:, "CT_Ger"] = dp["CT_SE"] + dp["CT_Aj"]

    print(f"✅ Offset neutralized: {offset_value:,.2f}")
    print(f"✅ Budget used for remaining side: {total_used:,.2f}")
    print(f"📊 Final budget usage: {total_used / remaining_budget * 100:+.1f}%")

    # --- Update final reconciled quantities and costs after adjustment ---
    dp["Qt_Ger"] = dp["Qt_SE"] + dp["Qt_Aj"]
    dp["CT_Ger"] = dp["CT_SE"] + dp["CT_Aj"]


    return dp

# -----------------------
# CLI / Runner
# -----------------------

def main(year: int, month: int, save_excel: bool = True) -> Path:
    this_dir = resolve_month_dir(year, month)
    if not this_dir:
        raise FileNotFoundError(f"Could not resolve base dir for {yymm_to_str(year, month)}")

    out_dir = this_dir
    ensure_dir(out_dir)

    # Run reconciliation (child level)
    report = reconcile_inventory(year, month)

    # Load parent mapping
    tables_dir = resolve_tables_dir(year, month)
    prodf = load_prodf(tables_dir)

    # Build parent-level aggregation
    first_cols = ["Qt_E", "CU_F", "CU_Pai"]  # add any others you want

    agg_map = {}
    for col in report.columns:
        if col in ["CODPF","CU_F", "CODPP"]:
            continue
        elif col in first_cols:
            agg_map[col] = "first"   # keep as is (same for all children under same parent)
        elif pd.api.types.is_numeric_dtype(report[col]):
            agg_map[col] = "sum"
        else:
            agg_map[col] = "first"

    parent_report = report

    # Define colunas na ordem exata da aba 'Parent' desejada
    final_cols_order = [
        "CODPP", "Ins", "Qt_I", "Qt_E", "Qt_S", "Qt_SE", "Qt_SS", "Qt_Diff", "Qt_Ger",
        "CT_I", "CT_E", "CT_S", "CT_SE", "CT_SS", "CT_Diff", "CT_Ger",
        "CU_I", "CU_E", "CU_S", "CU_F",
        "VQt_2b", "VQt_2c", "VQt_tot",
        "VV_2b", "VV_2c", "VV_tot",
        "Mrg_2b", "Mrg_2c", "Mrg_tot",
        "MrgPct_2b", "MrgPct_2c", "MrgPct_tot"
    ]
    # Filtra colunas existentes (caso alguma falte)
    parent_report = parent_report[[col for col in final_cols_order if col in parent_report.columns]]
    parent_report = adjust_missing_inventory_progressive(parent_report)

    # Save CSV and XLSX
    tag = yymm_to_str(year, month)
    xlsx_path = out_dir / f"Conc_Estoq_{tag}.xlsx"

    if save_excel:
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
            parent_report.to_excel(writer, index=False, sheet_name="Conc")
            wb = writer.book
            ws_child = writer.sheets["Conc"]
            apply_excel_formatting(ws_child, parent_report, wb)

    print(f"Saved: {xlsx_path}")
    return xlsx_path

if __name__ == "__main__":
    import argparse
    from datetime import datetime

    ap = argparse.ArgumentParser(description="Inventory reconciliation for a given year/month.")
    ap.add_argument("--year", "-y", type=int, help="Year, e.g. 2025")
    ap.add_argument("--month", "-m", type=int, help="Month, 1-12")
    args = ap.parse_args()

    # If missing, ask interactively
    if args.year is None or args.month is None:
        now = datetime.now()
        print("Year and/or month not provided.")
        year = int(input(f"Enter year (default {now.year}): ") or now.year)
        month = int(input(f"Enter month [1-12] (default {now.month -1}): ") or (now.month -1))
    else:
        year, month = args.year, args.month

    main(year, month)
