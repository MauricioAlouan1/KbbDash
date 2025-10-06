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
    df = pd.read_excel(file_path, dtype={'Pai': str, 'Filho': str})
    df["AnoMes"] = pd.to_numeric(df["AnoMes"], errors="coerce").astype("Int64")
    df["CUE"] = pd.to_numeric(df["CUE"], errors="coerce")
    df["CUPF"] = pd.to_numeric(df["CUPF"], errors="coerce")
    return df

def build_prior_cupf_lookup(df: pd.DataFrame, target_ano_mes: int) -> pd.DataFrame:
    prior = df[df["AnoMes"].notna() & (df["AnoMes"] < target_ano_mes)].copy()
    return (
        prior.sort_values(["Pai", "AnoMes"])
             .drop_duplicates("Pai", keep="last")[["Pai", "CUPF"]]
             .rename(columns={"CUPF": "CUPF_prior"})
    )

def apply_cupi_values(df: pd.DataFrame, target_ano_mes: int) -> pd.DataFrame:
    prior_cupf = build_prior_cupf_lookup(df, target_ano_mes)
    df = df.merge(prior_cupf, on="Pai", how="left")
    df["CUPI_calc"] = df["CUPF_prior"].where(df["CUPF_prior"].notna(), df["CUE"])
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

    target_rows = df[df["AnoMes"] == ano_mes]
    written = 0

    print(f"\n📝 Linhas escritas em '{column_name}' (AnoMes = {ano_mes}):")
    for idx, row in target_rows.iterrows():
        excel_row = idx + 2

        # 🟢 Se values_series tem índice igual ao df → usa idx
        # 🟢 Se values_series tem índice = Pai → usa row['Pai']
        if idx in values_series.index:
            val = values_series.loc[idx]
        else:
            pai = row.get("Pai", row.get("Codigo", None))
            val = values_series.get(pai, None)

        ws.cell(row=excel_row, column=col_map[column_name],
                value=float(val) if pd.notna(val) else 0)
        written += 1
        print(f"→ Linha Excel {excel_row}: Pai={row.get('Pai', row.get('Codigo','???'))}, {column_name}={val}")

    wb.save(out_path)
    return written

def apply_qtip_values(df: pd.DataFrame, target_ano_mes: int, base_dir: str, year: int, month: int) -> pd.DataFrame:
    prev_year, prev_month = get_prev_month(year, month)
    prev_df = load_previous_qtgerx(base_dir, prev_year, prev_month)

    df["Pai"] = df["Pai"].astype(str).str.strip()
    prev_df["CodPP"] = prev_df["CodPP"].astype(str).str.strip()

    df = df.merge(prev_df, left_on="Pai", right_on="CodPP", how="left")
    df["QtIP_calc"] = df["QtGerx"].fillna(0)
    return df

def calculate_qtsp_from_resumo(base_dir: str, year: int, month: int) -> pd.DataFrame:
    resumo_path = os.path.join(base_dir, "clean", f"{year}_{month:02d}", f"R_Resumo_{year}_{month:02d}.xlsm")
    ano_mes = (year % 100) * 100 + month
    print(f"Processando Saídas pro mês: {ano_mes}")
    vendas = []

    if os.path.exists(resumo_path):
        # O_NFCI
        df_nfci = pd.read_excel(resumo_path, sheet_name="O_NFCI")
        df_nfci["CODPP"] = df_nfci["CODPP"].astype(str).str.upper().str.strip()
        df_nfci["QTD"] = pd.to_numeric(df_nfci["QTD"], errors="coerce").fillna(0)
        df_nfci["ANOMES"] = pd.to_numeric(df_nfci["ANOMES"], errors="coerce")

        # 🔹 Filtra pelo AnoMes atual
        vendas.append(df_nfci[["CODPP", "QTD"]])

        # L_LPI
        df_lpi = pd.read_excel(resumo_path, sheet_name="L_LPI")
        df_lpi = df_lpi[df_lpi["STATUS PEDIDO"].astype(str).str.upper() != "CANCELADO"]
        df_lpi = df_lpi[df_lpi["EMPRESA"].astype(str).str.upper() == "K"]
        df_lpi["CODPP"] = df_lpi["CODPP"].astype(str).str.upper().str.strip()
        df_lpi["QTD"] = pd.to_numeric(df_lpi["QTD"], errors="coerce").fillna(0)
        df_lpi["ANOMES"] = pd.to_numeric(df_lpi["ANOMES"], errors="coerce")

        # 🔹 Filtra pelo AnoMes atual
        vendas.append(df_lpi[["CODPF", "QTD"]])

    if not vendas:
        raise ValueError(f"Nenhum dado de vendas encontrado para AnoMes {ano_mes} em 'O_NFCI' ou 'L_LPI'.")

    # Junta tudo
    df_all = pd.concat(vendas, axis=0)
    df_all = df_all.groupby("CODPP", as_index=False)["QTD"].sum().rename(columns={"QTD": "QtSP"})

    df_all["Pai"] = df_all["CODPP"].astype(str).str.upper().str.strip()

    # Agrupa por Pai
    final_df = df_all.groupby("Pai", as_index=False)["QtSP"].sum()
    print(final_df.head())

    return final_df
def get_prev_month(year: int, month: int) -> tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1

def load_previous_qtgerx(base_dir: str, prev_year: int, prev_month: int) -> pd.DataFrame:
    tag = f"{prev_year:04d}_{prev_month:02d}"
    file_path = os.path.join(base_dir, "clean", tag, f"R_Estoq_fdm_{tag}.xlsx")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ Previous month file not found: {file_path}")
    
    df = pd.read_excel(file_path, sheet_name="PT_pp", dtype={"Pai": str})
    df["CodPP"] = df["Pai"].astype(str).str.strip()
    df["QtGerx"] = pd.to_numeric(df["QtGerx"], errors="coerce").fillna(0)
    return df[["CodPP", "QtGerx"]]


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
    df = apply_cupi_values(df, ano_mes)
    df = apply_qtip_values(df, ano_mes, base_dir, year, month)
    # Calcular QtSP por Pai
    qtsp_df = calculate_qtsp_from_resumo(base_dir, year, month)
    # Mapear valores de QtSP baseados na coluna 'Pai'
    qtsp_map = qtsp_df.set_index("Pai")["QtSP"].to_dict()
    df["QtSP"] = df["Pai"].map(qtsp_map)

    aligned_qtsp_series = df.set_index("Pai").index.map(qtsp_df.set_index("Pai")["QtSP"].to_dict())
    df["QtSP"] = aligned_qtsp_series
    written = write_column_to_excel(
        df, excel_path=file_path, out_path=out_path,
        ano_mes=ano_mes, column_name="CUPI", values_series=df["CUPI_calc"]
    )
    print(f"✅ Wrote {written} CUPI values for AnoMes {ano_mes}")    
 
    written_qtip = write_column_to_excel(
        df, excel_path=file_path, out_path=out_path,
        ano_mes=ano_mes, column_name="QtIP", values_series=df["QtIP_calc"]
    )
    print(f"✅ Wrote {written_qtip} QtIP values for AnoMes {ano_mes}")

    # Escrever no Excel
    #print("\n📦 Debug QtSP: valores a escrever:")
    #print(qtsp_df[["Pai", "QtSP"]].to_string(index=False))

    written_qtsp = write_column_to_excel(
        df, excel_path=file_path,
        out_path=out_path,
        ano_mes=ano_mes,
        column_name="QtSP",
        values_series=qtsp_df.set_index("Pai")["QtSP"]
    )
    print(f"✅ Wrote {written_qtsp} QtSP values for AnoMes {ano_mes}")

    print(f"💾 Saved to: {out_path}")
# ─────────────────────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
