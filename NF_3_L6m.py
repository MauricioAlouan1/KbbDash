import os
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# === CONFIG ===
YEAR = 2025
MONTH = 10
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

# === FUNCTION ===
def combine_last_6_months(year: int, month: int):
    # Define output directory
    output_dir = os.path.join(BASE_FOLDER, "Mauricio", "Contabilidade - Tsuriel")
    
    all_data = []
    for i in range(6):
        target_date = datetime(year, month, 1) - relativedelta(months=i)
        y_str = target_date.strftime("%Y")
        m_str = target_date.strftime("%m")

        # find actual file
        pattern_prefix = f"Combined_NFs_{y_str}_{m_str}"
        found_file = None
        
        # Check if output_dir exists before listing
        if os.path.exists(output_dir):
            for f in os.listdir(output_dir):
                if f.startswith(pattern_prefix) and f.endswith(".xlsx"):
                    found_file = f
                    break
        
        if not found_file:
            print(f"⚠️ Missing file for {y_str}-{m_str}: {pattern_prefix}*.xlsx in {output_dir}")
            continue

        file_path = os.path.join(output_dir, found_file)
        try:
            df = pd.read_excel(file_path)
            df.insert(0, "RefMonth", f"{y_str}-{m_str}")
            all_data.append(df)
            print(f"📂 Added {found_file} ({len(df)} rows)")
        except Exception as e:
            print(f"❌ Error reading {found_file}: {e}")

    if not all_data:
        print("No data found for last 6 months — nothing to combine.")
        return

    combined_df = pd.concat(all_data, ignore_index=True)
    out_file = os.path.join(output_dir, f"Combined_NFs_L6M_{YEAR}_{MONTH:02d}.xlsx")
    combined_df.to_excel(out_file, index=False)

    print(f"\n✅ Combined 6-month Excel created: {out_file}")
    print(f"📊 Total rows combined: {len(combined_df)}")

# === RUN ===
if __name__ == "__main__":
    combine_last_6_months(YEAR, MONTH)
