import os
import pandas as pd
from openpyxl import load_workbook

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

# === Get target period ===
year, month = _prompt_year_month()
ano_mes = (year % 100) * 100 + month
print(f"▶ Target AnoMes = {ano_mes}")

# === Resolve base dir ===
path_options = [
    '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/',
    '/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data'
]
for path in path_options:
    if os.path.exists(path):
        base_dir = path
        break
else:
    print("❌ None of the specified directories exist.")
    exit()

tables_dir = os.path.join(base_dir, "Tables")
file_path = os.path.join(tables_dir, "T_Entradas.xlsx")

if not os.path.exists(file_path):
    print(f"❌ File not found: {file_path}")
    exit()

# === Load with pandas ===
df = pd.read_excel(file_path, dtype={'Pai': str, 'Filho': str})
df["AnoMes"] = pd.to_numeric(df["AnoMes"], errors="coerce").astype("Int64")
df["CUE"] = pd.to_numeric(df["CUE"], errors="coerce")
df["CUPF"] = pd.to_numeric(df["CUPF"], errors="coerce")

# Build lookup: latest CUPF per Pai before the target AnoMes
prior_rows = df[df["AnoMes"].notna() & (df["AnoMes"] < ano_mes)].copy()
prior_rows = (
    prior_rows.sort_values(["Pai", "AnoMes"])
              .drop_duplicates("Pai", keep="last")[["Pai", "CUPF"]]
              .rename(columns={"CUPF": "CUPF_prior"})
)

# Merge back to original dataframe to apply values to matching AnoMes
df = df.merge(prior_rows, on="Pai", how="left")

# Define values to write: if prior CUPF exists, use it; else use own CUE
df["CUPI_calc"] = df["CUPF_prior"].where(df["CUPF_prior"].notna(), df["CUE"])

# === Load workbook to write only the changed cells ===
wb = load_workbook(file_path)
ws = wb.active

# Header map
headers = [cell.value for cell in ws[1]]
col_map = {str(h): i + 1 for i, h in enumerate(headers)}

# Ensure CUPI column exists (if not, add at end)
if "CUPI" not in col_map:
    new_col = len(headers) + 1
    ws.cell(row=1, column=new_col, value="CUPI")
    col_map["CUPI"] = new_col

cupi_col = col_map["CUPI"]

# Write CUPI to rows matching current AnoMes
written = 0
for idx, row in df[df["AnoMes"] == ano_mes].iterrows():
    excel_row = idx + 2  # account for header
    val = row["CUPI_calc"]
    ws.cell(row=excel_row, column=cupi_col, value=float(val) if pd.notna(val) else None)
    written += 1

# Save output file
out_path = os.path.join(tables_dir, "T_Entradas_modified.xlsx")
wb.save(out_path)

print(f"✅ Wrote CUPI for {written} row(s) at AnoMes {ano_mes}")
print(f"💾 Saved: {out_path}")
