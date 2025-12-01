import os
import pandas as pd
from openpyxl import load_workbook

# === CONFIG ===
YEAR = "2025"
MONTH = "10-Outubro"

# Define base folder and available series
path_options = [
    '/Users/mauricioalouan/Dropbox/nfs',
    '/Users/simon/Library/CloudStorage/Dropbox/nfs'
]
BASE_FOLDER = None
for path in path_options:
    if os.path.exists(path):
        BASE_FOLDER = path
        break

if not BASE_FOLDER:
    print("⚠️ Warning: No valid BASE_FOLDER found.")
    BASE_FOLDER = "/Users/mauricioalouan/Dropbox/nfs"

# The same series used in Tabela_NFs.py
SERIES_LIST = [
    "Serie 1 - Omie",
    "Serie 2 - filial",
    "Serie 3 - Bling",
    "Serie 4 - Lexos",
    "Serie 5 - Olist",
    "Serie 6 - Meli",
    "Serie 7 - Amazon",
    "Serie 8 - Magalu Full",
    "Serie 9 - Shopee Full"
]

# === MAIN FUNCTION ===
def combine_monthly_excels(year, month):
    all_data = []
    for series in SERIES_LIST:
        filename = f"Extracted_Data_{year}_{month.replace('/', '-')}_{series}.xlsx"
        file_path = os.path.join(BASE_FOLDER, filename)
        
        if not os.path.exists(file_path):
            print(f"⚠️ Skipping missing file: {file_path}")
            continue
        
        try:
            df = pd.read_excel(file_path)
            df.insert(0, "Series", series)  # add series name
            all_data.append(df)
            print(f"📂 Added: {series} ({len(df)} rows)")
        except Exception as e:
            print(f"❌ Error reading {series}: {e}")
    
    if not all_data:
        print("No data files found — nothing to combine.")
        return
    
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_file = os.path.join(BASE_FOLDER, f"Combined_NFs_{year}_{month.replace('/', '-')}.xlsx")
    
    combined_df.to_excel(combined_file, index=False)
    print(f"✅ Combined Excel created: {combined_file}")
    print(f"📊 Total rows combined: {len(combined_df)}")

# === RUN ===
if __name__ == "__main__":
    combine_monthly_excels(YEAR, MONTH)
