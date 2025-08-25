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
CODPF, QT_INICIAL, CU_INICIAL, CT_INICIAL, VENDAS_2b, VENDAS_2c, QT_ENTRADAS, CU_ENTRADAS, CT_ENTRADAS, QT_FINAL, CU_FINAL, CT_FINAL
"""

from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
import numpy as np
from typing import List, Optional, Tuple

# -----------------------
# CONFIG – adjust these!
# -----------------------
# Two possible base folders, like your process_inv.py pattern
BASE_DIRS: List[Path] = [
    Path("/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/clean"),
    Path("/Users/mauricioalouan/KBDash01/data/clean"),
]

# Subfolder with Tables
TABLES_SUBPATH = "Tables"

# Output folder (inside the resolved base dir for the selected YYYY_MM)
OUTPUT_SUBDIR = "reports"

# -----------------------
# Helpers
# -----------------------

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

def resolve_month_dir(year: int, month: int) -> Optional[Path]:
    tag = yymm_to_str(year, month)
    # Many users keep month folders named as YYYY_MM under each base dir
    candidates = [base / tag for base in BASE_DIRS]
    return find_existing_file(candidates)

def resolve_tables_dir(year: int, month: int) -> Optional[Path]:
    # Tables might sit at the base root or inside YYYY_MM; try both patterns for both base dirs
    tag = yymm_to_str(year, month)
    candidates: List[Path] = []
    for base in BASE_DIRS:
        candidates.append(base / TABLES_SUBPATH)            # e.g., .../clean/Tables
        candidates.append(base / tag / TABLES_SUBPATH)      # e.g., .../clean/2025_07/Tables
    return find_existing_file(candidates)

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

def load_inventory_pt01(file_path: Path) -> pd.DataFrame:
    """
    Reads PT01 sheet and normalizes to columns: CODPF, QT, CU, CT
    Tries multiple common header variants.
    """
    df = read_excel_safe(file_path, sheet_name="PT01")
    # Try to locate code column
    code_col = first_existing_col(df, ["CODPF", "Código do Produto", "Codigo do Produto", "Pai", "CODIGO", "CÓDIGO"])
    if not code_col:
        raise ValueError(f"CODE column not found in PT01 of {file_path.name}")

    # Quantity column candidates
    qty_col = first_existing_col(df, ["QT", "QTDE", "QUANTIDADE", "Qt", "Quantidade", "Qt_Final", "ESTOQUE"])
    # Unit cost column candidates
    cu_col  = first_existing_col(df, ["CU", "CUSTO UNIT", "CUSTO_UNIT", "Ult CU R$", "Custo Unitário", "CUSTO UNITÁRIO"])
    # Total cost column candidates
    ct_col  = first_existing_col(df, ["CT", "CUSTO TOT", "CUSTO_TOTAL", "Custo Total", "CUSTO TOTAL", "Valor Estoque"])

    if qty_col is None:
        # Sometimes only total exists; assume 0 qty if missing
        qty_col = "QT_FALLBACK"
        df[qty_col] = 0
    if cu_col is None:
        cu_col = "CU_FALLBACK"
        df[cu_col] = 0.0
    if ct_col is None:
        # compute CT if we have CU*QT
        ct_col = "CT_COMPUTED"
        df[ct_col] = coerce_numeric(df[cu_col]) * coerce_numeric(df[qty_col])

    out = pd.DataFrame({
        "CODPF": norm_code(df[code_col]),
        "QT": coerce_numeric(df[qty_col]),
        "CU": coerce_numeric(df[cu_col]),
        "CT": coerce_numeric(df[ct_col]),
    })
    # If CT missing or zero but we have CU & QT, recompute
    need_ct = (out["CT"] == 0) & ((out["CU"] != 0) | (out["QT"] != 0))
    out.loc[need_ct, "CT"] = out.loc[need_ct, "CU"] * out.loc[need_ct, "QT"]
    return out.groupby("CODPF", as_index=False).agg({"QT":"sum", "CT":"sum", "CU":"mean"})

def load_sales_onfci(resumo_path: Path) -> pd.DataFrame:
    """
    Reads R_Resumo_YYYY_MM.xlsx sheet 'O_NFCI' and returns VENDAS_2b per CODPF.
    """
    df = read_excel_safe(resumo_path, sheet_name="O_NFCI")
    code_col = first_existing_col(df, ["CODPF", "Código do Produto", "Codigo do Produto", "Pai", "CODIGO"])
    if not code_col:
        # fallback: try SKU or Item fields sometimes present
        code_col = first_existing_col(df, ["SKU", "Item", "Produto"])
    if not code_col:
        raise ValueError(f"CODE column not found in O_NFCI of {resumo_path.name}")

    qty_col = first_existing_col(df, ["QT", "QTDE", "QUANTIDADE", "Quantidade", "Qt"])
    if not qty_col:
        # Some reports store sales quantity under different field
        qty_col = first_existing_col(df, ["QTD", "Qde"])
    if not qty_col:
        raise ValueError(f"Quantity column not found in O_NFCI of {resumo_path.name}")

    out = pd.DataFrame({
        "CODPF": norm_code(df[code_col]),
        "VENDAS_2b": coerce_numeric(df[qty_col]),
    })
    return out.groupby("CODPF", as_index=False).agg({"VENDAS_2b":"sum"})

def load_sales_llpi(resumo_path: Path) -> pd.DataFrame:
    """
    Reads R_Resumo_YYYY_MM.xlsx sheet 'L_LPI', filters STATUS PEDIDO != 'CANCELADO' and EMPRESA == 'K',
    and returns VENDAS_2c per CODPF.
    """
    df = read_excel_safe(resumo_path, sheet_name="L_LPI")

    # Apply filters
    status_col  = first_existing_col(df, ["STATUS PEDIDO", "Status Pedido", "STATUS_PEDIDO", "Status"])
    empresa_col = first_existing_col(df, ["EMPRESA", "Empresa"])
    if status_col:
        df = df[df[status_col].astype(str).str.upper() != "CANCELADO"]
    if empresa_col:
        df = df[df[empresa_col].astype(str).str.upper() == "K"]

    code_col = first_existing_col(df, ["CODPF", "Código do Produto", "Codigo do Produto", "Pai", "CODIGO", "SKU"])
    if not code_col:
        raise ValueError(f"CODE column not found in L_LPI of {resumo_path.name}")

    qty_col = first_existing_col(df, ["QT", "QTDE", "QUANTIDADE", "Quantidade", "Qt", "QTD", "Qde"])
    if not qty_col:
        raise ValueError(f"Quantity column not found in L_LPI of {resumo_path.name}")

    out = pd.DataFrame({
        "CODPF": norm_code(df[code_col]),
        "VENDAS_2c": coerce_numeric(df[qty_col]),
    })
    return out.groupby("CODPF", as_index=False).agg({"VENDAS_2c":"sum"})

def load_entradas(tables_dir: Path, year: int, month: int) -> pd.DataFrame:
    """
    Reads Tables/T_Entradas.xlsx, filters to target YYYY_MM (if present via 'AnoMes' or date col),
    and returns arrivals per CODPF: QT_ENTRADAS, CU_ENTRADAS (weighted avg), CT_ENTRADAS.
    """
    entradas_path = tables_dir / "T_Entradas.xlsx"
    if not entradas_path.exists():
        # Some teams keep a CSV backup
        csv_alt = tables_dir / "T_Entradas.csv"
        if not csv_alt.exists():
            raise FileNotFoundError(f"Could not find T_Entradas.xlsx or .csv under {tables_dir}")
        df = pd.read_csv(csv_alt)
    else:
        df = read_excel_safe(entradas_path)

    # CODE candidates
    code_col = first_existing_col(df, ["CODPF", "Pai", "Código do Produto", "Codigo do Produto", "CODIGO"])
    if not code_col:
        # occasionally arrives as 'Produto' or 'SKU'
        code_col = first_existing_col(df, ["Produto", "SKU"])
    if not code_col:
        raise ValueError("CODE column not found in T_Entradas")

    # Quantity candidates
    qty_col = first_existing_col(df, ["QT", "Qtde", "QUANTIDADE", "Quantidade", "QTD", "Qde", "ENT_QT"])
    if not qty_col:
        raise ValueError("Quantity column not found in T_Entradas")

    # Unit cost candidates
    cu_col = first_existing_col(df, ["CU", "Custo Unit", "Custo Unitário", "CU R$", "CustoUnit", "UnitCost"])
    # Total cost candidates
    ct_col = first_existing_col(df, ["CT", "Custo Total", "Valor Total", "CT R$", "TotalCost"])

    # Filter to target month if possible
    anomes_col = first_existing_col(df, ["AnoMes", "ANOMES"])
    if anomes_col:
        target = int(f"{year%100:02d}{month:02d}")  # yymm style
        df = df[pd.to_numeric(df[anomes_col], errors="coerce") == target]
    else:
        # Try by date column, if present
        date_col = first_existing_col(df, ["Data", "DATA", "Emissao", "EMISSAO", "Ultima Entrada"])
        if date_col:
            # Parse month/year
            parsed = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True, infer_datetime_format=True)
            df = df[ (parsed.dt.year == year) & (parsed.dt.month == month) ]

    work = pd.DataFrame({
        "CODPF": norm_code(df[code_col]),
        "QT": coerce_numeric(df[qty_col]),
    })
    if cu_col:
        work["CU"] = coerce_numeric(df[cu_col])
    else:
        work["CU"] = 0.0
    if ct_col:
        work["CT"] = coerce_numeric(df[ct_col])
    else:
        # compute CT if missing
        work["CT"] = work["QT"] * work["CU"]

    # Aggregate per item, compute weighted avg CU
    agg = work.groupby("CODPF", as_index=False).agg({"QT":"sum", "CT":"sum"})
    agg["CU"] = np.where(agg["QT"] != 0, agg["CT"] / agg["QT"], 0.0)
    agg = agg.rename(columns={
        "QT": "QT_ENTRADAS",
        "CU": "CU_ENTRADAS",
        "CT": "CT_ENTRADAS",
    })
    return agg

# -----------------------
# Main reconciliation
# -----------------------

def reconcile_inventory(year: int, month: int) -> pd.DataFrame:
    prev_y, prev_m = ym_to_prev(year, month)

    this_tag = yymm_to_str(year, month)
    prev_tag = yymm_to_str(prev_y, prev_m)

    # Resolve month dirs
    this_dir = resolve_month_dir(year, month)
    if not this_dir:
        raise FileNotFoundError(f"Could not resolve month dir for {this_tag} in any BASE_DIRS")

    prev_dir = resolve_month_dir(prev_y, prev_m)
    if not prev_dir:
        raise FileNotFoundError(f"Could not resolve month dir for {prev_tag} in any BASE_DIRS")

    tables_dir = resolve_tables_dir(year, month)
    if not tables_dir:
        raise FileNotFoundError("Could not resolve Tables directory in any BASE_DIRS")

    # Files
    prev_inv_path = prev_dir / f"R_Estoque_fdm_{prev_tag}.xlsx"
    this_inv_path = this_dir / f"R_Estoque_fdm_{this_tag}.xlsx"
    resumo_path   = this_dir / f"R_Resumo_{this_tag}.xlsx"

    for p in [prev_inv_path, this_inv_path, resumo_path]:
        if not p.exists():
            raise FileNotFoundError(f"Missing file: {p}")

    # Load datasets
    inv_prev = load_inventory_pt01(prev_inv_path)   # CODPF, QT, CU, CT (but CU here is avg; we trust CT)
    inv_this = load_inventory_pt01(this_inv_path)   # CODPF, QT, CU, CT
    vendas_b = load_sales_onfci(resumo_path)        # CODPF, VENDAS_2b
    vendas_c = load_sales_llpi(resumo_path)         # CODPF, VENDAS_2c
    entrs    = load_entradas(tables_dir, year, month)  # CODPF, QT_ENTRADAS, CU_ENTRADAS, CT_ENTRADAS

    # Normalize inventory columns to desired names
    inv_prev = inv_prev.rename(columns={"QT":"QT_INICIAL", "CU":"CU_INICIAL", "CT":"CT_INICIAL"})
    inv_this = inv_this.rename(columns={"QT":"QT_FINAL",   "CU":"CU_FINAL",   "CT":"CT_FINAL"})

    # Merge universe of items
    universe = (
        pd.DataFrame({"CODPF": pd.Series(dtype=str)})
        .pipe(lambda df:_merge_outer(df, inv_prev, "inv_prev"))
        .pipe(lambda df:_merge_outer(df, inv_this, "inv_this"))
        .pipe(lambda df:_merge_outer(df, vendas_b, "vendas_b"))
        .pipe(lambda df:_merge_outer(df, vendas_c, "vendas_c"))
        .pipe(lambda df:_merge_outer(df, entrs,    "entrs"))
    )

    # Fill NaNs with 0 where numeric
    numeric_cols = ["QT_INICIAL","CU_INICIAL","CT_INICIAL",
                    "VENDAS_2b","VENDAS_2c",
                    "QT_ENTRADAS","CU_ENTRADAS","CT_ENTRADAS",
                    "QT_FINAL","CU_FINAL","CT_FINAL"]
    for c in numeric_cols:
        if c in universe.columns:
            universe[c] = coerce_numeric(universe[c])

    # If CT missing but QT & CU present, compute
    if "CT_INICIAL" in universe.columns:
        m = (universe["CT_INICIAL"]==0) & ((universe["QT_INICIAL"]!=0) | (universe["CU_INICIAL"]!=0))
        universe.loc[m, "CT_INICIAL"] = universe.loc[m, "QT_INICIAL"] * universe.loc[m, "CU_INICIAL"]
    if "CT_FINAL" in universe.columns:
        m = (universe["CT_FINAL"]==0) & ((universe["QT_FINAL"]!=0) | (universe["CU_FINAL"]!=0))
        universe.loc[m, "CT_FINAL"] = universe.loc[m, "QT_FINAL"] * universe.loc[m, "CU_FINAL"]

    # Ensure sales columns exist
    for c in ["VENDAS_2b","VENDAS_2c"]:
        if c not in universe.columns:
            universe[c] = 0.0

    # Ensure entradas columns exist
    for c in ["QT_ENTRADAS","CU_ENTRADAS","CT_ENTRADAS"]:
        if c not in universe.columns:
            universe[c] = 0.0

    # Order and return
    cols = ["CODPF", "QT_INICIAL", "CU_INICIAL", "CT_INICIAL",
            "VENDAS_2b", "VENDAS_2c",
            "QT_ENTRADAS", "CU_ENTRADAS", "CT_ENTRADAS",
            "QT_FINAL", "CU_FINAL", "CT_FINAL"]
    # Some items might be missing; reindex safely
    existing = [c for c in cols if c in universe.columns]
    out = universe[["CODPF"] + [c for c in cols if c in existing and c!="CODPF"]].copy()
    out = out.sort_values("CODPF", kind="stable").reset_index(drop=True)
    return out

def _merge_outer(acc: pd.DataFrame, df: pd.DataFrame, tag: str) -> pd.DataFrame:
    # Expect df has CODPF and some measures
    if acc.empty:
        return df.copy()
    if "CODPF" not in acc.columns:
        acc = pd.DataFrame({"CODPF": pd.Series(dtype=str)}).merge(acc, how="outer", on="CODPF")
    return pd.merge(acc, df, on="CODPF", how="outer")

# -----------------------
# CLI / Runner
# -----------------------

def main(year: int, month: int, save_excel: bool = True) -> Path:
    this_dir = resolve_month_dir(year, month)
    if not this_dir:
        raise FileNotFoundError(f"Could not resolve base dir for {yymm_to_str(year, month)}")

    out_dir = this_dir / OUTPUT_SUBDIR
    ensure_dir(out_dir)

    report = reconcile_inventory(year, month)

    # Save CSV and XLSX
    tag = yymm_to_str(year, month)
    csv_path = out_dir / f"Recon_{tag}.csv"
    xlsx_path = out_dir / f"Recon_{tag}.xlsx"

    report.to_csv(csv_path, index=False, encoding="utf-8-sig")
    if save_excel:
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
            report.to_excel(writer, index=False, sheet_name="Recon")
            # Basic formatting
            wb = writer.book
            ws = writer.sheets["Recon"]
            for i, col in enumerate(report.columns):
                width = max(12, min(32, report[col].astype(str).str.len().max() if not report.empty else 12))
                ws.set_column(i, i, width)

    print(f"Saved: {csv_path}")
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
        month = int(input(f"Enter month [1-12] (default {now.month}): ") or now.month)
    else:
        year, month = args.year, args.month

    main(year, month)
