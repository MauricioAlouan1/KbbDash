import os
import pandas as pd
from datetime import datetime, timedelta

# === CONFIG ===
DEFAULT_START_YEAR = 2025
DEFAULT_START_MONTH = 1

# Calculate default end date (previous month)
today = datetime.today()
first_day_this_month = today.replace(day=1)
last_month_date = first_day_this_month - timedelta(days=1)
DEFAULT_END_YEAR = last_month_date.year
DEFAULT_END_MONTH = last_month_date.month

# Define potential base directories
path_options = [
    '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/',
    '/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/'
]

# Find the correct base directory
BASE_DIR = None
for path in path_options:
    if os.path.exists(path):
        BASE_DIR = path
        break

if BASE_DIR is None:
    print("❌ Error: Base directory not found.")
    exit(1)

print(f"✅ Base directory set to: {BASE_DIR}")

def get_user_input(prompt, default_value):
    user_input = input(f"{prompt} [{default_value}]: ").strip()
    if not user_input:
        return default_value
    try:
        return int(user_input)
    except ValueError:
        print("⚠️ Invalid input. Using default.")
        return default_value

def get_months_range(start_year, start_month, end_year, end_month):
    months = []
    current_year = start_year
    current_month = start_month

    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        months.append((current_year, current_month))
        
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
            
    return months

def main():
    print("\n=== Conc_Estoque_MultiPer Configuration ===")
    start_year = get_user_input("Start Year", DEFAULT_START_YEAR)
    start_month = get_user_input("Start Month", DEFAULT_START_MONTH)
    end_year = get_user_input("End Year", DEFAULT_END_YEAR)
    end_month = get_user_input("End Month", DEFAULT_END_MONTH)

    print(f"\nProcessing range: {start_month:02}/{start_year} to {end_month:02}/{end_year}")

    months_to_process = get_months_range(start_year, start_month, end_year, end_month)
    all_data = []

    for year, month in months_to_process:
        folder_path = os.path.join(BASE_DIR, "clean", f"{year:04}_{month:02}")
        filename = f"Conc_Estoq_{year:04}_{month:02}.xlsx"
        file_path = os.path.join(folder_path, filename)

        if not os.path.exists(file_path):
            print(f"⚠️ File not found: {file_path}")
            continue

        try:
            print(f"📖 Reading: {filename}")
            df = pd.read_excel(file_path)
            
            # Add anomes column
            anomes_val = int(f"{str(year)[-2:]}{month:02}") # e.g. 2501
            df.insert(0, "anomes", anomes_val)
            
            all_data.append(df)
            print(f"✅ Loaded {len(df)} rows.")
            
        except Exception as e:
            print(f"❌ Error reading {filename}: {e}")

    if not all_data:
        print("\n❌ No data collected. Exiting.")
        return

    print("\n🔄 Stacking data...")
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"📊 Total rows stacked: {len(combined_df)}")

    # Define output file
    output_folder = os.path.join(BASE_DIR, "clean", f"{end_year:04}_{end_month:02}")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder, exist_ok=True)
        
    start_str = f"{str(start_year)[-2:]}{start_month:02}"
    end_str = f"{str(end_year)[-2:]}{end_month:02}"
    output_filename = f"Conc_Estoq_Stacked_{start_str}_{end_str}.xlsx"
    output_path = os.path.join(output_folder, output_filename)

    print(f"💾 Saving to: {output_path}")
    combined_df.to_excel(output_path, index=False)
    print("✅ Done!")

if __name__ == "__main__":
    main()
