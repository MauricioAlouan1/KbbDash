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
# ─────────────────────────────────────────────────────────────
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
    for idx, row in target_rows.iterrows():
        excel_row = idx + 2
        val = values_series.loc[idx]
        ws.cell(row=excel_row, column=col_map[column_name], value=float(val) if pd.notna(val) else None)
        written += 1

    wb.save(out_path)
    return written

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

    written = write_column_to_excel(
        df, excel_path=file_path, out_path=out_path,
        ano_mes=ano_mes, column_name="CUPI", values_series=df["CUPI_calc"]
    )

    print(f"✅ Wrote {written} CUPI values for AnoMes {ano_mes}")
    print(f"💾 Saved to: {out_path}")
# ─────────────────────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
