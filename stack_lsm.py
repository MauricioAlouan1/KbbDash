import os
import pandas as pd
import shutil
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows


# Global variables
timeframe = 6  # Default: Last 6 months
ano_x = 2025
mes_x = 1

# Base directory
base_dir = "/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/clean"

# Function to get file paths for the last `timeframe` months
def get_last_n_files(base_dir, ano_x, mes_x, n):
    file_paths = []
    
    for i in range(n):
        # Compute the year and month dynamically
        year = ano_x
        month = mes_x - i

        if month <= 0:  # Adjust for year change
            month += 12
            year -= 1

        folder = os.path.join(base_dir, f"{year:04}_{month:02}")
        filename = f"R_Resumo_{year:04}_{month:02}.xlsm"
        file_path = os.path.join(folder, filename)

        if os.path.exists(file_path):
            file_paths.append(file_path)
        else:
            print(f"⚠️ Warning: File {file_path} not found. Skipping...")

    return file_paths

# Function to stack sheets from multiple files
import psutil  # To check memory usage

def stack_sheets(file_paths):
    stacked_data = {}
    first_file = True  # Track the first file (latest month)

    for file_path in file_paths:
        print(f"\n📂 Attempting to load file: {file_path}")

        try:
            if first_file:
                print(f"🔍 Opening {file_path} with VBA macros (KEEP FORMATTING)...")
                wb = load_workbook(file_path, keep_vba=True, read_only=False)  # ✅ KEEP FORMATTING for the latest file
                print(f"✅ Workbook loaded with macros: {file_path}")
                first_file = False
            else:
                print(f"🔍 Opening {file_path} WITHOUT macros (FASTER)...")
                wb = load_workbook(file_path, keep_vba=False, read_only=True)  # ✅ Faster loading for older files
                print(f"✅ Workbook loaded without macros: {file_path}")

        except Exception as e:
            print(f"❌ Error loading {file_path}: {e}")
            continue

        # Check memory usage before reading sheets
        print(f"🔍 Memory before reading sheets: {psutil.virtual_memory().percent}%")

        for sheet_name in wb.sheetnames:
            if sheet_name.startswith("Pivot"):  # Skip Pivot sheets
                continue

            print(f"🔹 Checking sheet: {sheet_name} in {file_path}")  # Debug before loading

            try:
                # Load only the first 100 rows for debugging
                df = pd.read_excel(file_path, sheet_name=sheet_name, engine="openpyxl", nrows=100)
                print(f"✅ Loaded first 100 rows of {sheet_name}: {df.shape}")  

                # If successful, now load full data
                df = pd.read_excel(file_path, sheet_name=sheet_name, engine="openpyxl")
                print(f"✅ Fully loaded {sheet_name}: {df.shape}")

                # Ensure stacked data structure
                if sheet_name not in stacked_data:
                    stacked_data[sheet_name] = df
                else:
                    if list(stacked_data[sheet_name].columns) == list(df.columns):
                        stacked_data[sheet_name] = pd.concat([stacked_data[sheet_name], df], ignore_index=True)
                        print(f"🔄 Stacked {sheet_name}: {stacked_data[sheet_name].shape}")
                    else:
                        print(f"⚠️ Column mismatch in {sheet_name}. Skipping...")
            except Exception as e:
                print(f"❌ Error reading {sheet_name}: {e}")

        # Check memory usage after reading sheets
        print(f"🔍 Memory after reading sheets: {psutil.virtual_memory().percent}%")

    return stacked_data

# Function to save the stacked data
def save_stacked_data(stacked_data, output_path, template_file):
    print(f"✅ Copying template {template_file} to {output_path} (preserving macros)...")
    shutil.copy(template_file, output_path)

    wb = load_workbook(output_path, keep_vba=True)

    # Remove existing sheets
    for sheet in wb.sheetnames:
        del wb[sheet]

    # Write stacked data
    for sheet_name, df in stacked_data.items():
        ws = wb.create_sheet(title=sheet_name)
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)
        print(f"✅ Added stacked sheet: {sheet_name}")

    # Save final output
    wb.save(output_path)
    print(f"✅ Stacked data saved as: {output_path}")

# Main function
def main():
    # Get last `timeframe` files
    file_paths = get_last_n_files(base_dir, ano_x, mes_x, timeframe)

    if not file_paths:
        print("❌ No valid files found. Exiting...")
        return

    # Stack sheets from all months
    stacked_data = stack_sheets(file_paths)

    # Save to the last month's folder
    last_month_folder = os.path.dirname(file_paths[0])
    output_file = os.path.join(last_month_folder, f"R_ResumoU6M_{ano_x:04}_{mes_x:02}.xlsm")

    # Use the latest file as a template
    template_file = file_paths[0]

    save_stacked_data(stacked_data, output_file, template_file)

# Run script
if __name__ == "__main__":
    main()
