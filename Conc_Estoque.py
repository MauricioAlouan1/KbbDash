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
PT01_CODE_COL = "Codigo_Inv"                # ex.: "CODPF" ou "Código do Produto"
PT01_QTY_COL  = "Total"                     # ex.: "QT" ou "Qt_Final" ou "ESTOQUE"
PT01_CU_COL   = "Unit Cost"                 # ex.: "Ult CU R$" ou "CU"
PT01_CT_COL   = "Total Cost"                # ex.: "Custo Total" ou "CT" (pode deixar None e o script calcula CU*QT)
# Se não existir, coloque None (o script calcula CT = CU*QT)

# O_NFCI (vendas 2b)
ONFCI_CODE_COL = "CODPF"               # ex.: "CODPF" ou "Código do Produto"
ONFCI_QTY_COL  = "QTD"                  # ex.: "QT", "Quantidade", "QTD"

# L_LPI (vendas 2c)
LLPI_CODE_COL    = "CODPF"
LLPI_QTY_COL     = "QTD"
LLPI_STATUS_COL  = "STATUS PEDIDO"     # usado para filtrar != "CANCELADO"
LLPI_EMPRESA_COL = "EMPRESA"           # usado para filtrar == "K"

# T_Entradas (entradas do mês)
ENTR_CODE_COL   = "Filho"              # ex.: "CODPF", "Pai"
ENTRP_CODE_COL   = "Pai"              # ex.: "CODPF", "Pai"
ENTR_QTY_COL    = "Qt"                 # ex.: "QT", "Qtde"
ENTR_CU_COL     = "Ult CU R$"                 # ex.: "CU" (se não tiver, use None)
ENTR_CT_COL     = "Ult CT"                 # ex.: "CT" (se não tiver, use None e calcula QT*CU)
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

def load_inventory_pt01(file_path: Path) -> pd.DataFrame:
    df = pd.read_excel(file_path, sheet_name=SHEET_PT01)

    # <<< HARD-CODE — usa exatamente as colunas definidas no topo
    code_col = PT01_CODE_COL
    qty_col  = PT01_QTY_COL
    cu_col   = PT01_CU_COL
    ct_col   = PT01_CT_COL

    # Normaliza tipos
    def _num(s):
        return pd.to_numeric(df[s], errors="coerce").fillna(0) if s else None

    CODPF = df[code_col].astype(str).str.strip().str.upper()
    QT = _num(qty_col) if qty_col else pd.Series(0, index=df.index, dtype="float")
    CU = _num(cu_col) if cu_col else pd.Series(0.0, index=df.index, dtype="float")
    if ct_col:
        CT = _num(ct_col)
    else:
        CT = QT * CU  # calcula se não veio pronto

    out = pd.DataFrame({"CODPF": CODPF, "QT": QT, "CU": CU, "CT": CT})
    # se CT = 0 mas QT e CU existem, recomputa
    m = (out["CT"] == 0) & ((out["QT"] != 0) | (out["CU"] != 0))
    out.loc[m, "CT"] = out.loc[m, "QT"] * out.loc[m, "CU"]

    # Consolida por item
    agg = out.groupby("CODPF", as_index=False).agg({"QT":"sum", "CT":"sum"})
    agg["CU"] = np.where(agg["QT"] != 0, agg["CT"]/agg["QT"], 0.0)
    return agg

def load_sales_onfci(resumo_path: Path) -> pd.DataFrame:
    df = pd.read_excel(resumo_path, sheet_name=SHEET_ONFCI)

    CODPF = df[ONFCI_CODE_COL].astype(str).str.strip().str.upper()
    QT    = pd.to_numeric(df[ONFCI_QTY_COL], errors="coerce").fillna(0)
    VV    = pd.to_numeric(df["MERCVLR"], errors="coerce").fillna(0)   # sales value
    Mrg   = pd.to_numeric(df["MARGVLR"], errors="coerce").fillna(0)   # contribution margin

    out = pd.DataFrame({
        "CODPF": CODPF,
        "VENDAS_2b": QT,
        "VV_2b": VV,
        "Mrg_2b": Mrg
    })
    return out.groupby("CODPF", as_index=False).agg({
        "VENDAS_2b":"sum",
        "VV_2b":"sum",
        "Mrg_2b":"sum"
    })

def load_sales_llpi(resumo_path: Path) -> pd.DataFrame:
    df = pd.read_excel(resumo_path, sheet_name=SHEET_LLPI)

    # Filtros fixos
    df = df[df[LLPI_STATUS_COL].astype(str).str.upper() != "CANCELADO"]
    df = df[df[LLPI_EMPRESA_COL].astype(str).str.upper() == "K"]

    CODPF = df[LLPI_CODE_COL].astype(str).str.strip().str.upper()
    QT    = pd.to_numeric(df[LLPI_QTY_COL], errors="coerce").fillna(0)
    VV    = pd.to_numeric(df["VLRVENDA"], errors="coerce").fillna(0)
    Mrg   = pd.to_numeric(df["MargVlr"], errors="coerce").fillna(0)

    out = pd.DataFrame({
        "CODPF": CODPF,
        "VENDAS_2c": QT,
        "VV_2c": VV,
        "Mrg_2c": Mrg
    })
    return out.groupby("CODPF", as_index=False).agg({
        "VENDAS_2c":"sum",
        "VV_2c":"sum",
        "Mrg_2c":"sum"
    })


def load_entradas(tables_dir: Path, year: int, month: int) -> pd.DataFrame:
    entradas_path = tables_dir / ENTRADAS_FILE
    if not entradas_path.exists():
        raise FileNotFoundError(f"Arquivo de entradas não encontrado: {entradas_path}")

    df = pd.read_excel(entradas_path)

    # Filtragem por mês
    if ENTR_ANOMES_COL:
        target = int(f"{year%100:02d}{month:02d}")  # yymm
        df = df[pd.to_numeric(df[ENTR_ANOMES_COL], errors="coerce") == target]
    elif ENTR_DATE_COL:
        parsed = pd.to_datetime(df[ENTR_DATE_COL], errors="coerce", dayfirst=True, infer_datetime_format=True)
        df = df[(parsed.dt.year == year) & (parsed.dt.month == month)]
    # senão, não filtra (usa tudo)

    CODPF = df[ENTR_CODE_COL].astype(str).str.strip().str.upper()
    QT    = pd.to_numeric(df[ENTR_QTY_COL], errors="coerce").fillna(0)

    if ENTR_CT_COL:
        CT = pd.to_numeric(df[ENTR_CT_COL], errors="coerce").fillna(0)
    else:
        if ENTR_CU_COL:
            CU_src = pd.to_numeric(df[ENTR_CU_COL], errors="coerce").fillna(0)
            CT = QT * CU_src
        else:
            CT = pd.Series(0.0, index=df.index, dtype="float")

    work = pd.DataFrame({"CODPF": CODPF, "QT": QT, "CT": CT})
    agg = work.groupby("CODPF", as_index=False).agg({"QT":"sum", "CT":"sum"})
    agg["CU"] = np.where(agg["QT"] != 0, agg["CT"]/agg["QT"], 0.0)
    return agg.rename(columns={"QT":"QT_ENTRADAS","CU":"CU_ENTRADAS","CT":"CT_ENTRADAS"})

def load_cu_pai(tables_dir: Path, year: int, month: int) -> pd.DataFrame:
    entradas_path = tables_dir / ENTRADAS_FILE
    if not entradas_path.exists():
        raise FileNotFoundError(f"Arquivo de entradas não encontrado: {entradas_path}")

    df = pd.read_excel(entradas_path)

    # Ensure date/period column
    if ENTR_ANOMES_COL:
        df["ANOMES"] = pd.to_numeric(df[ENTR_ANOMES_COL], errors="coerce")
        cutoff = int(f"{year%100:02d}{month:02d}")  # yymm
        df = df[df["ANOMES"] <= cutoff]
    elif ENTR_DATE_COL:
        parsed = pd.to_datetime(df[ENTR_DATE_COL], errors="coerce", dayfirst=True, infer_datetime_format=True)
        cutoff = pd.to_datetime(f"{year}-{month:02d}-01") + pd.offsets.MonthEnd(0)
        df = df[parsed <= cutoff]
        df["ANOMES"] = parsed.dt.year % 100 * 100 + parsed.dt.month
    else:
        raise ValueError("Nem ENTR_ANOMES_COL nem ENTR_DATE_COL definidos.")

    # Normalize codes
    df["CODPF"] = df[ENTRP_CODE_COL].astype(str).str.strip().str.upper()
    CU_src = pd.to_numeric(df[ENTR_CU_COL], errors="coerce").fillna(0)

    # Attach parent codes
    tables_dir = Path(tables_dir)
    prodf = load_prodf(tables_dir)
    df = df.merge(prodf, on="CODPF", how="left")

    # Sort so latest entries come first
    df = df.sort_values(["CODPP","ANOMES"], ascending=[True, False])

    # Pick last CU for each parent
    latest = df.drop_duplicates("CODPP", keep="first")[["CODPP", ENTR_CU_COL]].rename(columns={ENTR_CU_COL: "CU_Pai"})

    return latest

# -----------------------
# Main reconciliation
# -----------------------

def reconcile_inventory(year: int, month: int) -> pd.DataFrame:
    """
    Conciliação de estoque para (year, month).
    Depende das funções/utilitários já definidos:
      - resolve_month_dir, resolve_tables_dir
      - load_inventory_pt01, load_sales_onfci, load_sales_llpi, load_entradas
      - INV_PREFIX, RESUMO_PREFIX
    Retorna DataFrame com colunas:
      CODPF, QT_INICIAL, CU_INICIAL, CT_INICIAL, VENDAS_2b, VENDAS_2c,
      QT_ENTRADAS, CU_ENTRADAS, CT_ENTRADAS, QT_FINAL, CU_FINAL, CT_FINAL
    """
    # Mês anterior
    prev_y = year if month > 1 else year - 1
    prev_m = month - 1 if month > 1 else 12

    this_tag = f"{year:04d}_{month:02d}"
    prev_tag = f"{prev_y:04d}_{prev_m:02d}"

    # Pastas hard-coded
    this_dir = resolve_month_dir(year, month)
    prev_dir = resolve_month_dir(prev_y, prev_m)
    tables_dir = resolve_tables_dir(year, month)

    # Arquivos hard-coded a partir dos prefixos
    prev_inv_path = find_existing_excel(prev_dir, f"{INV_PREFIX}{prev_tag}")
    this_inv_path = find_existing_excel(this_dir,  f"{INV_PREFIX}{this_tag}")
    resumo_path   = find_existing_excel(this_dir,  f"{RESUMO_PREFIX}{this_tag}")

    for p in [prev_inv_path, this_inv_path, resumo_path]:
        if not p.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {p}")

    # Carrega dados
    inv_prev = load_inventory_pt01(prev_inv_path)   # CODPF, QT, CU, CT
    inv_this = load_inventory_pt01(this_inv_path)   # CODPF, QT, CU, CT
    vendas_b = load_sales_onfci(resumo_path)        # CODPF, VENDAS_2b
    vendas_c = load_sales_llpi(resumo_path)         # CODPF, VENDAS_2c
    entrs    = load_entradas(tables_dir, year, month)  # CODPF, QT_ENTRADAS, CU_ENTRADAS, CT_ENTRADAS
    prodf = load_prodf(tables_dir)

    inv_prev = inv_prev.merge(prodf, on="CODPF", how="left")
    inv_this = inv_this.merge(prodf, on="CODPF", how="left")
    vendas_b = vendas_b.merge(prodf, on="CODPF", how="left")
    vendas_c = vendas_c.merge(prodf, on="CODPF", how="left")
    entrs    = entrs.merge(prodf, on="CODPF", how="left")

    # Renomeia inventários p/ INICIAL/FINAL
    inv_prev = inv_prev.rename(columns={"QT": "QT_INICIAL", "CU": "CU_INICIAL", "CT": "CT_INICIAL"})
    inv_this = inv_this.rename(columns={"QT": "QT_FINAL",   "CU": "CU_FINAL",   "CT": "CT_FINAL"})

    # Merge universo
    # Começa pelo conjunto que sempre existe (inv_prev) e vai encostando os demais
    df = inv_prev.copy()
    for add in [inv_this, vendas_b, vendas_c, entrs]:
        if add is not None and not add.empty:
            df = pd.merge(df, add, on=["CODPF","CODPP"], how="outer")

    if "CODPF" not in df.columns:
        # Caso extremo (todas as fontes vazias): força coluna
        df["CODPF"] = pd.Series(dtype=str)

    # Garante tipos numéricos e preenche NaN
    num_cols = [
        "QT_INICIAL", "CU_INICIAL", "CT_INICIAL",
        "VENDAS_2b", "VENDAS_2c",
        "QT_ENTRADAS", "CU_ENTRADAS", "CT_ENTRADAS",
        "QT_FINAL", "CU_FINAL", "CT_FINAL",
        # 👇 add your new ones
        "VV_2b","VV_2c","VV_tot",
        "Mrg_2b","Mrg_2c","Mrg_tot",
        "MrgPct_2b","MrgPct_2c","MrgPct_tot"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Recalcula CT se vier 0 com QT/CU disponíveis
    if "CT_INICIAL" in df.columns:
        m = (df["CT_INICIAL"] == 0) & ((df["QT_INICIAL"] != 0) | (df["CU_INICIAL"] != 0))
        df.loc[m, "CT_INICIAL"] = df.loc[m, "QT_INICIAL"] * df.loc[m, "CU_INICIAL"]

    if "CT_FINAL" in df.columns:
        m = (df["CT_FINAL"] == 0) & ((df["QT_FINAL"] != 0) | (df["CU_FINAL"] != 0))
        df.loc[m, "CT_FINAL"] = df.loc[m, "QT_FINAL"] * df.loc[m, "CU_FINAL"]

    # Garante existência das colunas pedidas
    for c in num_cols:
        if c not in df.columns:
            df[c] = 0.0

    # Ordena colunas na saída final
    cols_base = [
        "CODPF", "CODPP",
        "QT_INICIAL", "CU_INICIAL", "CT_INICIAL",
        "VENDAS_2b", "VENDAS_2c", "VENDAS_tot",   # 👈 put Vendas_Tot here
        "QT_ENTRADAS", "CU_ENTRADAS", "CT_ENTRADAS",
        "QT_FINAL", "CU_FINAL", "CT_FINAL",
    ]

    # add new metrics here
    cols_extra = [
        "VV_2b","VV_2c","VV_tot",
        "Mrg_2b","Mrg_2c","Mrg_tot",
        "MrgPct_2b","MrgPct_2c","MrgPct_tot",
        "QT_Diff","CU_Diff","CT_Diff_QT","CT_Diff_CU"
    ]

    # keep only what exists in df (in case of partial data)
    final_cols = [c for c in cols_base + cols_extra if c in df.columns]
    df = df[final_cols].copy()

    # Calculadas
    df["VENDAS_tot"]  = df["VENDAS_2b"] + df["VENDAS_2c"]

    # New value and margin metrics
    df["VV_tot"]     = df.get("VV_2b", 0) + df.get("VV_2c", 0)
    df["Mrg_tot"]    = df.get("Mrg_2b", 0) + df.get("Mrg_2c", 0)

    # Percentages (protect divide by zero)
    vv2b = df["VV_2b"] if "VV_2b" in df.columns else pd.Series(0, index=df.index)
    mrg2b = df["Mrg_2b"] if "Mrg_2b" in df.columns else pd.Series(0, index=df.index)

    vv2c = df["VV_2c"] if "VV_2c" in df.columns else pd.Series(0, index=df.index)
    mrg2c = df["Mrg_2c"] if "Mrg_2c" in df.columns else pd.Series(0, index=df.index)

    vvtot = df["VV_tot"] if "VV_tot" in df.columns else pd.Series(0, index=df.index)
    mrgtot = df["Mrg_tot"] if "Mrg_tot" in df.columns else pd.Series(0, index=df.index)

    df["MrgPct_2b"]  = np.where(vv2b != 0, mrg2b / vv2b, 0)
    df["MrgPct_2c"]  = np.where(vv2c != 0, mrg2c / vv2c, 0)
    df["MrgPct_tot"] = np.where(vvtot != 0, mrgtot / vvtot, 0)

    # cap at -100%
    for c in ["MrgPct_2b","MrgPct_2c","MrgPct_tot"]:
        df[c] = df[c].apply(lambda x: max(x, -1))
    
    # Merge CU_Pai (last known parent cost up to month)
    cu_pai = load_cu_pai(tables_dir, year, month)
    df = df.merge(cu_pai, on="CODPP", how="left")

    df["QT_Diff"]     = df["QT_FINAL"] - (df["QT_INICIAL"] - df["VENDAS_2b"] - df["VENDAS_2c"] + df["QT_ENTRADAS"])
    df["CU_Diff"]     = np.where(df["QT_FINAL"] > 0, df["CU_FINAL"] - df["CU_INICIAL"], 0)
    df["CT_Diff_QT"]  = df["QT_Diff"] * df["CU_Pai"]
    df["CT_Diff_CU"]  = np.where(
        (df["QT_INICIAL"] > 0) & (df["QT_FINAL"] > 0),
        df["QT_INICIAL"] * (df["CU_FINAL"] - df["CU_INICIAL"]),
        0
    )
        # Arredondamento (2 casas decimais)
    df["CU_Diff"]     = df["CU_Diff"].round(2)
    df["CT_Diff_QT"]  = df["CT_Diff_QT"].round(2)
    df["CT_Diff_CU"]  = df["CT_Diff_CU"].round(2)

    # Ordena por código
    df["CODPF"] = df["CODPF"].astype(str).str.strip().str.upper()
    df = df.sort_values("CODPF", kind="stable").reset_index(drop=True)
    return df

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

    out_dir = this_dir
    ensure_dir(out_dir)

    # Run reconciliation (child level)
    report = reconcile_inventory(year, month)

    # Load parent mapping
    tables_dir = resolve_tables_dir(year, month)
    prodf = load_prodf(tables_dir)

    # Build parent-level aggregation

    agg_map = {}
    for col in report.columns:
        if col in ["CODPF", "CODPP"]:
            continue
        elif col == "CU_Pai":
            agg_map[col] = "first"   # keep as is (same for all children under same parent)
        elif pd.api.types.is_numeric_dtype(report[col]):
            agg_map[col] = "sum"
        else:
            agg_map[col] = "first"

    parent_report = (
        report.groupby("CODPP", as_index=False)
        .agg(agg_map)
    )

    # Recompute CU from CT/QT at parent level
    if "QT_INICIAL" in parent_report.columns and "CT_INICIAL" in parent_report.columns:
        parent_report["CU_INICIAL"] = np.where(
            parent_report["QT_INICIAL"] != 0,
            parent_report["CT_INICIAL"] / parent_report["QT_INICIAL"],
            0.0,
        )
    if "QT_FINAL" in parent_report.columns and "CT_FINAL" in parent_report.columns:
        parent_report["CU_FINAL"] = np.where(
            parent_report["QT_FINAL"] != 0,
            parent_report["CT_FINAL"] / parent_report["QT_FINAL"],
            0.0,
        )

    # Save CSV and XLSX
    tag = yymm_to_str(year, month)
    xlsx_path = out_dir / f"Conc_Estoq_{tag}.xlsx"

    if save_excel:
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
            # --- Child sheet ---
            report.to_excel(writer, index=False, sheet_name="Child")
            wb = writer.book
            ws_child = writer.sheets["Child"]

            # --- Parent sheet ---
            parent_report.to_excel(writer, index=False, sheet_name="Parent")
            ws_parent = writer.sheets["Parent"]

            # Define formats
            money_fmt       = wb.add_format({'num_format': '#,##0.00'})
            light_gray_fmt  = wb.add_format({'bg_color': '#F2F2F2'})
            money_gray_fmt  = wb.add_format({'num_format': '#,##0.00', 'bg_color': '#F2F2F2'})
            blue_money_fmt  = wb.add_format({'num_format': '#,##0.00', 'bg_color': '#DDEBF7'})   # light blue
            green_money_fmt = wb.add_format({'num_format': '#,##0.00', 'bg_color': '#E2EFDA'})   # light green
            blue_pct_fmt    = wb.add_format({'num_format': '0%',       'bg_color': '#DDEBF7'})   # blue % format
            orange_money_fmt= wb.add_format({'num_format': '#,##0.00', 'bg_color': '#FCE4D6'})   # light orange

            money_cols = [
                "CU_INICIAL", "CT_INICIAL",
                "CU_ENTRADAS", "CT_ENTRADAS",
                "CU_FINAL", "CT_FINAL",
                "CU_Diff", "CT_Diff_QT", "CT_Diff_CU"
            ]
            gray_cols = {"QT_Diff", "CU_Diff", "CT_Diff_QT", "CT_Diff_CU"}

            # Apply formatting to both sheets
            for ws, df in [(ws_child, report), (ws_parent, parent_report)]:
                ws.autofilter(0, 0, df.shape[0], df.shape[1] - 1)
                for idx, col in enumerate(df.columns):
                    width = max(12, min(32, df[col].astype(str).str.len().max() if not df.empty else 12))
                    if col in gray_cols:
                        fmt = light_gray_fmt
                    elif col in money_cols:
                        fmt = money_fmt
                    elif col in {"VV_2b","VV_2c","VV_tot"}:
                        fmt = blue_money_fmt
                    elif col in {"Mrg_2b","Mrg_2c","Mrg_tot"}:
                        fmt = green_money_fmt
                    elif col in {"MrgPct_2b","MrgPct_2c","MrgPct_tot"}:
                        fmt = blue_pct_fmt
                    elif col in {"VENDAS_2b","VENDAS_2c","Vendas_Tot"}:
                        fmt = orange_money_fmt
                    else:
                        fmt = None
                    ws.set_column(idx, idx, width, fmt)

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
