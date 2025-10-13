import os
import pandas as pd
from openpyxl import load_workbook

# ─────────────────────────────────────────────────────────────
# 1. Prompt Logic
# ─────────────────────────────────────────────────────────────
def _prompt_year_month():
    import argparse
    from datetime import datetime
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--year", "-y", type=int)
    ap.add_argument("--month", "-m", type=int)
    args, _ = ap.parse_known_args()

    if args.year is not None and args.month is not None:
        return args.year, args.month

    now = datetime.now()
    default_year = now.year if now.month > 1 else now.year - 1
    default_month = now.month - 1 if now.month > 1 else 12
    print("Year and/or month not provided.")
    year = int(input(f"Enter year (default {default_year}): ") or default_year)
    month = int(input(f"Enter month [1-12] (default {default_month}): ") or default_month)
    return year, month

def resolve_base_dir():
    path_options = [
        '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/',
        '/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data'
    ]
    for path in path_options:
        if os.path.exists(path):
            return path
    raise FileNotFoundError("❌ None of the specified directories exist.")

# ─────────────────────────────────────────────────────────────
# 2. Data Loading and Processing
# ─────────────────────────────────────────────────────────────
def load_entrada_df(file_path: str) -> pd.DataFrame:
    wb = load_workbook(file_path, data_only=True)
    ws = wb.active
    data = ws.values
    columns = next(data)
    df = pd.DataFrame(data, columns=columns)

    if "Pai" in df.columns:
        df["Pai"] = df["Pai"].astype(str).str.strip()
    if "AnoMes" in df.columns:
        df["AnoMes"] = pd.to_numeric(df["AnoMes"], errors="coerce").astype("Int64")
    for col in ["CU_E", "CU_F"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def build_prior_cupf_lookup(df: pd.DataFrame, target_ano_mes: int) -> pd.DataFrame:
    prior = df[df["AnoMes"].notna() & (df["AnoMes"] < target_ano_mes)].copy()
    return (
        prior.sort_values(["Pai", "AnoMes"])
             .drop_duplicates("Pai", keep="last")[["Pai", "CU_F"]]
             .rename(columns={"CU_F": "CU_F_prior"})
    )

def apply_cupi_values(df: pd.DataFrame, target_ano_mes: int) -> pd.DataFrame:
    prior_cupf = build_prior_cupf_lookup(df, target_ano_mes)
    df = df.merge(prior_cupf, on="Pai", how="left")
    df["CU_I_new"] = df["CU_F_prior"].where(df["CU_F_prior"].notna(), df["CU_E"])
    return df

# ─────────────────────────────────────────────────────────────
# 3. Excel Writing (with formatting preserved)
# ────────────────────────────────────────────────────────
def write_column_to_excel(df: pd.DataFrame, excel_path: str, out_path: str,
                          ano_mes: int, column_name: str, values_series: pd.Series):
    wb = load_workbook(excel_path)
    ws = wb.active

    headers = [cell.value for cell in ws[1]]
    col_map = {str(h): i + 1 for i, h in enumerate(headers)}

    if column_name not in col_map:
        col_idx = len(headers) + 1
        ws.cell(row=1, column=col_idx, value=column_name)
        col_map[column_name] = col_idx

    # Normalize
    df["Pai"] = df["Pai"].astype(str).str.strip()
    values_series.index = values_series.index.astype(str).str.strip()

    written = 0
    print(f"\n📝 Linhas escritas em '{column_name}' (AnoMes = {ano_mes}):")

    # Loop through each row in Excel and update if Pai + AnoMes match
    for row_idx in range(2, ws.max_row + 1):
        cell_pai = str(ws.cell(row=row_idx, column=col_map.get("Pai", 0)).value or "").strip()
        cell_anomes = ws.cell(row=row_idx, column=col_map.get("AnoMes", 0)).value

        if str(cell_pai) in values_series.index and cell_anomes == ano_mes:
            val = values_series.get(cell_pai)
            if pd.notna(val):
                ws.cell(row=row_idx, column=col_map[column_name], value=float(val))
                written += 1
                print(f"→ Linha Excel {row_idx}: Pai={cell_pai}, {column_name}={val}")

    wb.save(out_path)
    return written

def apply_qtip_values(df: pd.DataFrame, target_ano_mes: int, base_dir: str, year: int, month: int) -> pd.DataFrame:
    prev_year, prev_month = get_prev_month(year, month)
    prev_df = load_previous_qtgerx(base_dir, prev_year, prev_month)

    df["Pai"] = df["Pai"].astype(str).str.strip()
    prev_df["CODPP"] = prev_df["CODPP"].astype(str).str.strip()

    df = df.merge(prev_df, left_on="Pai", right_on="CODPP", how="left")
    df["Qt_I"] = df["Qt_Ger"].fillna(0)
    return df

def calculate_qtsp_from_resumo(base_dir: str, year: int, month: int) -> pd.DataFrame:
    resumo_path = os.path.join(base_dir, "clean", f"{year}_{month:02d}", f"R_Resumo_{year}_{month:02d}.xlsm")
    ano_mes = (year % 100) * 100 + month
    print(f"Processando Saídas pro mês: {ano_mes}")
    vendas = []

    if not os.path.exists(resumo_path):
        raise FileNotFoundError(f"❌ Arquivo não encontrado: {resumo_path}")

    # ─────────────────────────────
    # O_NFCI (vendas tipo B)
    # ─────────────────────────────
    try:
        df_nfci = pd.read_excel(resumo_path, sheet_name="O_NFCI")
        df_nfci["CODPP"] = df_nfci["CODPP"].astype(str).str.upper().str.strip()
        df_nfci["QTD_B"] = pd.to_numeric(df_nfci["QTD"], errors="coerce").fillna(0)
        df_nfci["ANOMES"] = pd.to_numeric(df_nfci["ANOMES"], errors="coerce")
        df_nfci = df_nfci[df_nfci["ANOMES"] == ano_mes]
        vendas.append(df_nfci[["CODPP", "QTD_B"]])
    except Exception as e:
        print(f"⚠️ Erro ao ler O_NFCI: {e}")

    # ─────────────────────────────
    # L_LPI (vendas tipo C)
    # ─────────────────────────────
    try:
        df_lpi = pd.read_excel(resumo_path, sheet_name="L_LPI")
        df_lpi = df_lpi[df_lpi["STATUS PEDIDO"].astype(str).str.upper() != "CANCELADO"]
        df_lpi = df_lpi[df_lpi["EMPRESA"].astype(str).str.upper() == "K"]
        df_lpi["CODPP"] = df_lpi["CODPP"].astype(str).str.upper().str.strip()
        df_lpi["QTD_C"] = pd.to_numeric(df_lpi["QTD"], errors="coerce").fillna(0)
        df_lpi["ANOMES"] = pd.to_numeric(df_lpi["ANOMES"], errors="coerce")
        df_lpi = df_lpi[df_lpi["ANOMES"] == ano_mes]
        vendas.append(df_lpi[["CODPP", "QTD_C"]])
    except Exception as e:
        print(f"⚠️ Erro ao ler L_LPI: {e}")

    if not vendas:
        raise ValueError(f"❌ Nenhum dado de vendas encontrado para AnoMes {ano_mes}.")

    # ─────────────────────────────
    # Concatenate and sum properly
    # ─────────────────────────────
    df_all = pd.concat(vendas, ignore_index=True)

    # unify column names before summing
    if "QTD_B" not in df_all.columns:
        df_all["QTD_B"] = 0
    if "QTD_C" not in df_all.columns:
        df_all["QTD_C"] = 0

    # group by product and sum totals
    df_all = (
        df_all.groupby("CODPP", as_index=False)[["QTD_B", "QTD_C"]]
        .sum()
        .assign(Qt_S=lambda x: x["QTD_B"] + x["QTD_C"])
    )

    print(f"✅ {len(df_all)} produtos processados com Qt_S calculado.")
    print(df_all.head())

    return df_all[["CODPP", "Qt_S"]]

def get_prev_month(year: int, month: int) -> tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1

def load_previous_qtgerx(base_dir: str, prev_year: int, prev_month: int) -> pd.DataFrame:
    tag = f"{prev_year:04d}_{prev_month:02d}"
    file_path = os.path.join(base_dir, "clean", tag, f"Conc_Estoq_{tag}.xlsx")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ Previous month file not found: {file_path}")
    
    df = pd.read_excel(file_path, sheet_name="Conc", dtype={"CODPP": str})
    df["CODPP"] = df["CODPP"].astype(str).str.strip()
    df["Qt_Ger"] = pd.to_numeric(df["Qt_Ger"], errors="coerce").fillna(0)
    df["CU_F"] = pd.to_numeric(df["CU_F"], errors="coerce").fillna(0)
    return df[["CODPP", "Qt_Ger", "CU_F"]]

def apply_prev_month_values(df: pd.DataFrame, base_dir: str, year: int, month: int) -> pd.DataFrame:
    """
    Fills Qt_I and CU_I_new based on previous month's Conc_Estoq_<tag>.xlsx.
    - Qt_I = previous month's Qt_Ger
    - CU_I_new = previous month's CU_F
    - If not found: Qt_I = 0, CU_I_new = CU_E
    """
    import numpy as np

    prev_year, prev_month = get_prev_month(year, month)
    tag = f"{prev_year:04d}_{prev_month:02d}"
    file_path = os.path.join(base_dir, "clean", tag, f"Conc_Estoq_{tag}.xlsx")

    if not os.path.exists(file_path):
        print(f"⚠️ Arquivo anterior não encontrado: {file_path}")
        df["Qt_I"] = 0
        df["CU_I_new"] = df["CU_E"]
        return df

    print(f"🔁 Lendo dados do mês anterior: {file_path}")
    prev_df = pd.read_excel(file_path, sheet_name="Conc", dtype={"CODPP": str})
    prev_df["CODPP"] = prev_df["CODPP"].astype(str).str.strip().str.upper()
    prev_df["Qt_Ger"] = pd.to_numeric(prev_df["Qt_Ger"], errors="coerce").fillna(0)
    prev_df["CU_F"]   = pd.to_numeric(prev_df["CU_F"], errors="coerce").fillna(0)

    df["Pai"] = df["Pai"].astype(str).str.strip().str.upper()

    # Merge both Qt_Ger and CU_F
    df = df.merge(
        prev_df[["CODPP", "Qt_Ger", "CU_F"]].rename(columns={"CU_F": "CU_F_prev", "Qt_Ger": "Qt_Ger_prev"}),
        left_on="Pai", right_on="CODPP", how="left"
    )

    # Apply fallback logic for new items
    df["Qt_I"] = np.where(df["Qt_Ger_prev"].notna(), df["Qt_Ger_prev"], 0)
    df["CU_I_new"] = np.where(df["CU_F_prev"].notna() & (df["CU_F_prev"] > 0), df["CU_F_prev"], df["CU_E"])

    print(f"✅ Aplicados valores do mês anterior para {df['Qt_Ger_prev'].notna().sum()} itens existentes.")
    print(f"🆕 Itens novos com Qt_I=0 e CU_I=CU_E: {(df['Qt_Ger_prev'].isna()).sum()}")

    return df


# ─────────────────────────────────────────────────────────────
# 4. Main logic
# ─────────────────────────────────────────────────────────────
def main():
    year, month = _prompt_year_month()
    ano_mes = (year % 100) * 100 + month
    print(f"▶ Target AnoMes = {ano_mes}")

    base_dir = resolve_base_dir()
    tables_dir = os.path.join(base_dir, "Tables")
    file_path = os.path.join(tables_dir, "T_Entradas.xlsx")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ File not found: {file_path}")

    # Ask user whether to overwrite original
    choice = input("💾 Overwrite original file? [y/n]: ").strip().lower()
    if choice == "y":
        out_path = file_path
    elif choice == "n":
        out_path = os.path.join(tables_dir, "T_Entradas_modified.xlsx")
    else:
        print("❌ Aborted by user.")
        exit()

    df = load_entrada_df(file_path)
    print(f"\n🔍 Pai values in T_Entradas for AnoMes {ano_mes}:")
    print(df[df["AnoMes"] == ano_mes]["Pai"].dropna())

    df = apply_prev_month_values(df, base_dir, year, month)

    # Assign final columns for the current month
    df.loc[df["AnoMes"] == ano_mes, "CU_I"] = df.loc[df["AnoMes"] == ano_mes, "CU_I_new"]

    qtsp_df = calculate_qtsp_from_resumo(base_dir, year, month)

    # 🧩 Debug: check which Pai match between T_Entradas and Qt_S summary
    target_pais = df[df["AnoMes"] == ano_mes]["Pai"].dropna().astype(str).str.strip().unique()
    print(f"\n🔍 Pai values in T_Entradas for AnoMes {ano_mes}: {list(target_pais)}")

    matching_qtsp = qtsp_df[qtsp_df["CODPP"].isin(target_pais)].copy()
    #non_matching_qtsp = qtsp_df[~qtsp_df["Pai"].isin(target_pais)].copy()

    print("\n✅ Matching Pai found in Qt_S summary:")
    print(matching_qtsp if not matching_qtsp.empty else "⚠️ None found!")

    #print("\n❌ Non-matching Pai in Qt_S summary (first 20):")
    #print(non_matching_qtsp.head(20))

    # Mapear valores de Qt_S baseados na coluna 'Pai'
    qtsp_map = qtsp_df.set_index("CODPP")["Qt_S"].to_dict()
    df["Qt_S"] = df["Pai"].map(qtsp_map)

    # Make sure Pai is string in both DataFrame and values_series
    df["Pai"] = df["Pai"].astype(str).str.strip()

    qtsp_series = qtsp_df.copy()
    qtsp_series["CODPP"] = qtsp_series["CODPP"].astype(str).str.strip()
    qtsp_series = qtsp_series.set_index("CODPP")["Qt_S"]

    # 🔧 Normalize index before writing (avoid duplicate Pai and Series issues)
    df_unique = (
        df.loc[df["AnoMes"] == ano_mes, ["Pai", "CU_I_new", "Qt_I"]]
        .dropna(subset=["Pai"])
        .drop_duplicates(subset="Pai", keep="first")
        .set_index("Pai")
    )
    df_unique["CU_I_new"] = df_unique["CU_I_new"].round(3)
    df_unique["Qt_I"] = df_unique["Qt_I"].round(3)

    # Debug
    print("\n🧪 Debug preview before writing CU_I / Qt_I: - df_unique")
    print(df_unique.head(20))

    # Then use df_unique for both series:
    written = write_column_to_excel(
        df, excel_path=file_path, out_path=out_path,
        ano_mes=ano_mes, column_name="CU_I",
        values_series=df_unique["CU_I_new"]
    )

    written_qtip = write_column_to_excel(
        df, excel_path=out_path, out_path=out_path,
        ano_mes=ano_mes, column_name="Qt_I",
        values_series=df_unique["Qt_I"]
    )

    print(f"✅ Wrote {written_qtip} Qt_I values for AnoMes {ano_mes}")

    # Escrever no Excel
    #print("\n📦 Debug Qt_S: valores a escrever:")
    #print(qtsp_df[["Pai", "Qt_S"]].to_string(index=False))

    written_qtsp = write_column_to_excel(
        df, excel_path=out_path,
        out_path=out_path,
        ano_mes=ano_mes,
        column_name="Qt_S",
        values_series=qtsp_df.set_index("CODPP")["Qt_S"]
    )
    print(f"✅ Wrote {written_qtsp} Qt_S values for AnoMes {ano_mes}")

    print(f"💾 Saved to: {out_path}")
# ─────────────────────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
