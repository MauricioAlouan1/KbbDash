#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Conciliação de Contas a Receber para um mês e ano específicos.
Compara o saldo do mês anterior com o saldo atual, considerando:
- Faturas emitidas (O_NFCI)
- Pagamentos recebidos (O_CC), filtrando por "Rec Vendas"
- Saldos de contas a receber (O_CtasARec)
"""

import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Tuple

# Base de dados (ajuste conforme necessário)
path_options = [
    '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/',
    '/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data'
]
for candidate in path_options:
    if os.path.exists(candidate):
        base_dir = candidate
        break
else:
    print("Nenhum diretório base encontrado.")
    base_dir = None

CLEAN_ROOT = os.path.join(base_dir, "clean")

def yymm_to_str(year: int, month: int) -> str:
    return f"{year:04d}_{month:02d}"

def ym_to_prev(year: int, month: int) -> Tuple[int, int]:
    return (year - 1, 12) if month == 1 else (year, month - 1)

def resolve_month_dir(year: int, month: int) -> Path:
    tag = yymm_to_str(year, month)
    p = Path(os.path.join(CLEAN_ROOT, tag))
    if not p.exists():
        raise FileNotFoundError(f"Pasta do mês não encontrada: {p}")
    return p

def find_existing_excel(base_path: Path, base_name: str) -> Path:
    for ext in [".xlsx", ".xlsm"]:
        candidate = base_path / f"{base_name}{ext}"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Arquivo não encontrado: {base_path}/{base_name}.xlsx ou .xlsm")

def reconcile_receber(year: int, month: int) -> pd.DataFrame:
    prev_y, prev_m = ym_to_prev(year, month)
    tag     = yymm_to_str(year, month)
    prev_tag = yymm_to_str(prev_y, prev_m)

    this_dir = resolve_month_dir(year, month)
    prev_dir = resolve_month_dir(prev_y, prev_m)

    path_car_this = find_existing_excel(this_dir, f"O_CtasARec_{tag}_clean")
    path_car_prev = find_existing_excel(prev_dir, f"O_CtasARec_{prev_tag}_clean")
    path_cc       = find_existing_excel(this_dir, f"O_CC_{tag}_clean")
    path_nf       = find_existing_excel(this_dir, f"O_NFCI_{tag}_clean")

    # --- A Receber Inicial
    df_prev = pd.read_excel(path_car_prev)
    receberei_prev = df_prev.groupby("Razão Social", as_index=False)["A Receber"].sum()
    receberei_prev = receberei_prev.rename(columns={"Razão Social": "Razao Social", "A Receber": "A Receber Inicial"})

    # --- A Receber Final
    df_this = pd.read_excel(path_car_this)
    receberei_this = df_this.groupby("Razão Social", as_index=False)["A Receber"].sum()
    receberei_this = receberei_this.rename(columns={"Razão Social": "Razao Social", "A Receber": "A Receber Final"})

    # --- Faturado
    df_nf = pd.read_excel(path_nf)
    faturado = df_nf.groupby("Cliente (Razão Social)", as_index=False)["Total da Nota Fiscal"].sum()
    faturado = faturado.rename(columns={"Cliente (Razão Social)": "Razao Social", "Total da Nota Fiscal": "Faturado"})

    # --- Recebido (com filtro via T_CCCats)
    df_cc = pd.read_excel(path_cc)

    # Carrega T_CCCats e filtra
    tables_dir = Path(os.path.join(base_dir, "Tables"))
    cats_path = find_existing_excel(tables_dir, "T_CCCats")
    df_cats = pd.read_excel(cats_path)

    df_cc = pd.merge(
        df_cc,
        df_cats[["CC_Categoria Omie", "CC_Tipo"]],
        how="left",
        left_on="Categoria",
        right_on="CC_Categoria Omie"
    )

    df_cc = df_cc[df_cc["CC_Tipo"] == "Rec Vendas"]

    recebimentos = df_cc.groupby("Cliente ou Fornecedor (Razão Social)", as_index=False)["Valor (R$)"].sum()
    recebimentos = recebimentos.rename(columns={"Cliente ou Fornecedor (Razão Social)": "Razao Social", "Valor (R$)": "Recebido"})

    # --- Conciliação
    df = receberei_prev.copy()
    for add_df in [faturado, recebimentos, receberei_this]:
        df = pd.merge(df, add_df, on="Razao Social", how="outer")

    for col in ["A Receber Inicial", "Faturado", "Recebido", "A Receber Final"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["Diff"] = df["A Receber Inicial"] + df["Faturado"] - df["Recebido"] - df["A Receber Final"]
    df["Diff"] = df["Diff"].round(2)

    df = df.sort_values("Razao Social", kind="stable").reset_index(drop=True)
    return df

def main(year: int, month: int) -> Path:
    this_dir = resolve_month_dir(year, month)
    tag = yymm_to_str(year, month)

    out_path = this_dir / f"Conc_CARReceber_{tag}.xlsx"
    df = reconcile_receber(year, month)

    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="CARReceber")
        ws = writer.sheets["CARReceber"]
        ws.autofilter(0, 0, df.shape[0], df.shape[1] - 1)

    print(f"Arquivo salvo em: {out_path}")
    return out_path

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", "-y", type=int, help="Ano (ex: 2025)")
    ap.add_argument("--month", "-m", type=int, help="Mês (1-12)")
    args = ap.parse_args()

    if args.year is None or args.month is None:
        now = datetime.now()
        print("Ano e/ou mês não especificado.")
        year = int(input(f"Ano (default={now.year}): ") or now.year)
        month = int(input(f"Mês [1-12] (default={now.month - 1}): ") or (now.month - 1))
    else:
        year, month = args.year, args.month

    main(year, month)
