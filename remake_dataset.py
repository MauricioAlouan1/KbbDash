import pandas as pd
import os
from datetime import datetime, timedelta

def load_recent_data(base_dir, file_pattern, months=3):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=months * 30)  # Approximately three months
    frames = []
    for month_count in range(months + 1):  # Current month + last three months
        year_month = (start_date + timedelta(days=30 * month_count)).strftime('%Y_%m')
        subdir = os.path.join(base_dir, 'clean', year_month)
        file_path = os.path.join(subdir, file_pattern.format(year_month=year_month))
        if os.path.exists(file_path):
            print(f"Loading file: {file_path}")  # Debug print
            df = pd.read_excel(file_path)
            print(f"Loaded {file_path} with shape: {df.shape}")  # Debug print
            frames.append(df)
        else:
            print(f"File not found: {file_path}")  # Debug print
    return pd.concat(frames) if frames else pd.DataFrame()

def load_static_data(static_dir, filename):
    file_path = os.path.join(static_dir, filename)
    if os.path.exists(file_path):
        print(f"Loading static file: {file_path}")  # Debug print
        return pd.read_excel(file_path)
    else:
        print(f"Static file not found: {file_path}")  # Debug print
        return pd.DataFrame()

def merge_data_with_static(recent_data, static_data_dict):
    for key, static_df in static_data_dict.items():
        if key == "TabGeral":
            common_column = "CommonColumn"  # Replace with the actual column name you want to use for merging
            print(f"Attempting to merge on column: {common_column}")  # Debug print
            print(f"Recent data columns: {recent_data.columns}")  # Debug print
            print(f"Static data {key} columns: {static_df.columns}")  # Debug print
            if common_column in recent_data.columns and common_column in static_df.columns:
                print(f"Merging {key} on column: {common_column}")  # Debug print
                recent_data = recent_data.merge(static_df, on=common_column, how="left")
            else:
                print(f"Column '{common_column}' not found in both dataframes for merging.")  # Debug print
    return recent_data

def main():
    base_dir = '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/'
    static_dir = os.path.join(base_dir, 'Tables')
    
    # Define file patterns for each data type
    file_patterns = {
        'O_NFCI': 'O_NFCI_{year_month}_clean.xlsx',
        'B_Estoq': 'B_Estoq_{year_month}_clean.xlsx',
        'L_LPI': 'L_LPI_{year_month}_clean.xlsx',
        'MLA_Vendas': 'MLA_Vendas_{year_month}_clean.xlsx',
        'MLK_Vendas': 'MLK_Vendas_{year_month}_clean.xlsx',
        'O_CC': 'O_CC_{year_month}_clean.xlsx',
        'O_CtasAPagar': 'O_CtasAPagar_{year_month}_clean.xlsx',
        'O_CtasARec': 'O_CtasARec_{year_month}_clean.xlsx',
        'O_Estoq': 'O_Estoq_{year_month}_clean.xlsx',
        'O_NFSI': 'O_NFSI_{year_month}_clean.xlsx'
    }

    all_data = {}

    for key, pattern in file_patterns.items():
        recent_data = load_recent_data(base_dir, pattern)
        print(f"{key} data shape: {recent_data.shape}")  # Debug print
        all_data[key] = recent_data

    # Load static data
    static_tables = ['T_CondPagto.xlsx', 'T_Fretes.xlsx', 'T_GruposCli.xlsx', 'T_MP.xlsx', 
                     'T_RegrasMP.xlsx', 'T_Remessas.xlsx', 'T_Reps.xlsx', 'T_Verbas.xlsx', 'TabGeral.xlsx']
    static_data_dict = {table.replace('.xlsx', ''): load_static_data(static_dir, table) for table in static_tables}
    
    # Check static data shapes
    for key, df in static_data_dict.items():
        print(f"Static data {key} shape: {df.shape}")  # Debug print
    
    # Merge data with static data for O_NFCI as an example (expand as needed)
    all_data['O_NFCI'] = merge_data_with_static(all_data['O_NFCI'], static_data_dict)
    print(f"Merged O_NFCI data shape: {all_data['O_NFCI'].shape}")  # Debug print

    # Save all data to one Excel file with multiple sheets
    output_path = os.path.join(base_dir, 'clean', 'merged_data.xlsx')
    with pd.ExcelWriter(output_path) as writer:
        for key, df in all_data.items():
            df.to_excel(writer, sheet_name=key, index=False)
            print(f"Saved {key} data to {output_path} in sheet {key}")  # Debug print

    print(f"All merged data saved to {output_path}")

if __name__ == "__main__":
    main()
