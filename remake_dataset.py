import pandas as pd
import os
from datetime import datetime, timedelta

column_rename_dict = {
    'O_NFCI': {
        'Operação': 'Op',
        'Nota Fiscal': 'NF',
        'Data de Emissão (completa)': 'Emiss',
        'Cliente (Razão Social)': 'NomeRS',
        'Cliente (Nome Fantasia)': 'NomeF',
        'Código do Produto': 'CodPF', 
        'Quantidade': 'Qtd', 
        'Valor do ICMS ST': 'ICMSST',
        'Valor do IPI': 'IPI',
        'Total da Nota Fiscal': 'TotalNF',
 
        # Add other columns that need renaming for O_NFCI
    },
    'B_Estoq': {
        'Código': 'CodPF',
        # Add other columns that need renaming for B_Estoq
    },
    # Add dictionaries for other dataframes...
}

def rename_columns(all_data, rename_dict):
    for df_name, col_renames in rename_dict.items():
        if df_name in all_data:
            all_data[df_name] = all_data[df_name].rename(columns=col_renames)
            print(f"Renamed columns in {df_name}: {col_renames}")  # Debug print
    return all_data

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
    return pd.read_excel(os.path.join(static_dir, filename))

def standardize_text_case(df):
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.upper()
    return df

def merge_data(df1, df2, merge_on):
    return df1.merge(df2, on=merge_on, how='left')

def merge_all_data(all_data):
    #All UpperCase
    all_data = {key: standardize_text_case(df) for key, df in all_data.items()}

    # Example merge operations
    # Adjust these operations based on your specific merging needs

    # Merge O_NFCI with T_Remessas on column 'NomeF'
    if 'O_NFCI' in all_data and 'T_Remessas' in all_data:
        all_data['O_NFCI'] = all_data['O_NFCI'].merge(
            all_data['T_Remessas'][['NomeF']].drop_duplicates(), 
            on='NomeF', 
            how='left', 
            indicator='Remessa'
        )
        all_data['O_NFCI']['Remessa'] = all_data['O_NFCI']['Remessa'].apply(lambda x: 1 if x == 'both' else 0)
        print(f"Merged O_NFCI with T_Remessas: {all_data['O_NFCI'].shape}")



    return all_data

def add_computed_columns(all_data):
    for key, df in all_data.items():
        if 'Quantity' in df.columns and 'Price' in df.columns:
            df['TotalValue'] = df['Quantity'] * df['Price']
    return all_data

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
                     'T_RegrasMP.xlsx', 'T_Remessas.xlsx', 'T_Reps.xlsx', 'T_Verbas.xlsx','T_Vol.xlsx']
    static_data_dict = {table.replace('.xlsx', ''): load_static_data(static_dir, table) for table in static_tables}
    
    # Check static data shapes
    for key, df in static_data_dict.items():
        print(f"Static data {key} shape: {df.shape}")  # Debug print
    
    # Add static data to all_data dictionary
    all_data.update(static_data_dict)

    all_data = rename_columns(all_data, column_rename_dict)

    # Merge all data with static data
    all_data = merge_all_data(all_data)
    print(f"Merged data shapes:")
    for key, df in all_data.items():
        print(f"{key}: {df.shape}")  # Debug print

    # Add computed columns
    all_data = add_computed_columns(all_data)  # Add computed columns

    # Save all data to one Excel file with multiple sheets
    output_path = os.path.join(base_dir, 'clean', 'merged_data.xlsx')
    with pd.ExcelWriter(output_path) as writer:
        for key, df in all_data.items():
            df.to_excel(writer, sheet_name=key, index=False)
            print(f"Saved {key} data to {output_path} in sheet {key}")  # Debug print

    print(f"All merged data saved to {output_path}")

if __name__ == "__main__":
    main()
