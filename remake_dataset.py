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
        'Total da Nota Fiscal': 'TotalNF'
 
        # Add other columns that need renaming for O_NFCI
    },
    'B_Estoq': {
        'Código': 'CodPF'
        # Add other columns that need renaming for B_Estoq
    },
    # Add dictionaries for other dataframes...
}

def rename_columns(all_data, column_rename_dict):
    for df_name, rename_dict in column_rename_dict.items():
        if df_name in all_data:
            df = all_data[df_name]
            # Debug print to verify columns before renaming
            print(f"Before rename:\nTable: {df_name}\nColumns: {df.columns.tolist()}")
            df.rename(columns=rename_dict, inplace=True)
            # Debug print to verify columns after renaming
            print(f"Renamed columns in {df_name}: {rename_dict}")
            print(f"Columns in {df_name}: {df.columns.tolist()}")
            all_data[df_name] = df
    return all_data

def load_recent_data(base_dir, file_pattern, months=3):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=months * 30)  # Approximately three months
    frames = []
    for month_count in range(months + 1):  # Current month + last three months
        year_month = (start_date + timedelta(days=30 * month_count)).strftime('%Y_%m')
        file_path = os.path.join(base_dir, 'clean', year_month, file_pattern.format(year_month=year_month))
        if os.path.exists(file_path):
            df = pd.read_excel(file_path)
            frames.append(df)
            print(f"Loaded {file_path} with shape: {df.shape}")  # Debug print
        else:
            print(f"File not found: {file_path}")  # Debug print
    return pd.concat(frames) if frames else pd.DataFrame()

def load_static_data(static_dir, filename):
    return pd.read_excel(os.path.join(static_dir, filename))

def standardize_text_case(df):
    """Convert all text to uppercase for standardization."""
    if isinstance(df, pd.DataFrame):
        df.columns = [col.upper() for col in df.columns]
        for col in df.select_dtypes(include=[object]).columns:
            df[col] = df[col].str.upper()
    return df


def merge_all_data(all_data):
    # Ensure all relevant columns are in uppercase for case-insensitive comparison
    all_data = {key: standardize_text_case(df) for key, df in all_data.items()}

    print(f"Columns in O_NFCI: {all_data['O_NFCI'].columns}")
    print(f"Columns in T_Remessas: {all_data['T_Remessas'].columns}")
    print(f"Columns in T_Prodf: {all_data['T_Prodf'].columns}")
    print(f"Columns in T_GruposCli: {all_data['T_GruposCli'].columns}")

    # Merge O_NFCI with T_Remessas on column 'NomeF'
    all_data = merge_data(all_data, "O_NFCI", "NomeF", "T_Remessas", "NomeF", "Rem", default_value=0)

    # Merge O_NFCI with T_Prodf on column 'CodPF'
    all_data = merge_data(all_data, "O_NFCI", "CodPF", "T_Prodf", "CodPF", "CodPP")

    # Merge O_NFCI with T_GruposCli on column 'NomeF'
    all_data = merge_data(all_data, "O_NFCI", "NomeF", "T_GruposCli", "NomeF", "G1", default_value="V")

    # Merge inventory data with existing dataset (Example)
    all_data = merge_data(all_data, "O_NFCI", "CodPF", "INVENTORY_DATA", "CodPF", "Estoque")

    return all_data

def preprocess_inventory_data(file_path):
    sheets = pd.read_excel(file_path, sheet_name=None, header=1)  # Load data with headers from the second row
    processed_sheets = {}

    for sheet_name, df in sheets.items():
        df = df.melt(id_vars=['CodPF'], var_name='Date', value_name='Value')
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%y%m')
        processed_sheets[sheet_name] = df
    
    return processed_sheets

def merge_data(all_data, df1_name, df1_col, df2_name, df2_col, new_col=None, indicator_name=None, default_value=None):
    df1_col = df1_col.upper()
    df2_col = df2_col.upper()
    if new_col:
        new_col = new_col.upper()

    if df1_name in all_data and df2_name in all_data:
        df1 = all_data[df1_name]
        df2 = all_data[df2_name]

        # Standardize column names
        df1.columns = [col.upper() for col in df1.columns]
        df2.columns = [col.upper() for col in df2.columns]

        print(f"Columns in {df1_name} before merge: {df1.columns}")
        print(f"Columns in {df2_name} before merge: {df2.columns}")

        if df1_col not in df1.columns or df2_col not in df2.columns:
            raise KeyError(f"Column '{df1_col}' or '{df2_col}' not found in dataframes.")

        df2_cols = [df2_col] + ([new_col] if new_col else [])
        merged_df = df1.merge(df2[df2_cols].drop_duplicates(), left_on=df1_col, right_on=df2_col, how='left', indicator=indicator_name)

        if indicator_name and default_value is not None:
            merged_df[indicator_name] = merged_df[indicator_name].apply(lambda x: default_value if x == 'left_only' else merged_df[new_col])
            merged_df.drop(columns=[new_col, indicator_name], inplace=True)

        all_data[df1_name] = merged_df
    return all_data


def add_computed_columns(all_data):
    for key, df in all_data.items():
        if 'Quantity' in df.columns and 'Price' in df.columns:
            df['TotalValue'] = df['Quantity'] * df['Price']
    return all_data

def load_inventory_data(file_path):
    return pd.read_excel(file_path)

def print_all_tables_and_columns(all_data):
    for table_name, df in all_data.items():
        print(f"Table: {table_name}")
        print("Columns:", df.columns.tolist())
        print("-" * 50)

def main():
    base_dir = '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/'
    static_dir = os.path.join(base_dir, 'Tables')
    inventory_file_path = os.path.join(static_dir, 'R_EstoqComp.xlsx')  # Update to the correct path if needed

    # Define file patterns for each data type
    file_patterns = {
        'O_NFCI': 'O_NFCI_{year_month}_clean.xlsx',
        'O_NFSI': 'O_NFSI_{year_month}_clean.xlsx',
        'B_Estoq': 'B_Estoq_{year_month}_clean.xlsx',
        'L_LPI': 'L_LPI_{year_month}_clean.xlsx',
        'MLA_Vendas': 'MLA_Vendas_{year_month}_clean.xlsx',
        'MLK_Vendas': 'MLK_Vendas_{year_month}_clean.xlsx',
        'O_CC': 'O_CC_{year_month}_clean.xlsx',
        'O_CtasAPagar': 'O_CtasAPagar_{year_month}_clean.xlsx',
        'O_CtasARec': 'O_CtasARec_{year_month}_clean.xlsx',
        'O_Estoq': 'O_Estoq_{year_month}_clean.xlsx',
    }

    all_data = {}

    for key, pattern in file_patterns.items():
        recent_data = load_recent_data(base_dir, pattern)
        print(f"{key} data shape: {recent_data.shape}")  # Debug print
        all_data[key] = recent_data

    # Load static data
    static_tables = ['T_CondPagto.xlsx', 'T_Fretes.xlsx', 'T_GruposCli.xlsx', 'T_MP.xlsx', 
                     'T_RegrasMP.xlsx', 'T_Remessas.xlsx', 'T_Reps.xlsx', 'T_Verbas.xlsx','T_Vol.xlsx', 'T_Prodf.xlsx', 'T_ProdP.xlsx', 'T_Entradas.xlsx']
    static_data_dict = {table.replace('.xlsx', ''): load_static_data(static_dir, table) for table in static_tables}
    
    # Check static data shapes
    for key, df in static_data_dict.items():
        print(f"Static data {key} shape: {df.shape}")  # Debug print
    
    inventory_data = preprocess_inventory_data(inventory_file_path)

    print_all_tables_and_columns(all_data)
    print(f"-----xxxxxxxxxxxxxxxxx--------------")

    # Add static data to all_data dictionary
    #all_data.update(static_data_dict) #removed 
    all_data = {
        'O_NFCI': recent_data,
        **static_data_dict,
        **inventory_data
    }
    print_all_tables_and_columns(all_data)

    all_data = rename_columns(all_data, column_rename_dict)

    # Merge all data with static data
    all_data = merge_all_data(all_data) 
    #print(f"Merged data shapes:")
    #for key, df in all_data.items():
    #    print(f"{key}: {df.shape}")  # Debug print

    # Add computed columns
    all_data = add_computed_columns(all_data)  # Add computed columns

    # Save all data to one Excel file with multiple sheets
    output_path = os.path.join(base_dir, 'clean', 'merged_data.xlsx')
    with pd.ExcelWriter(output_path) as writer:
        for key, df in all_data.items():
            df.to_excel(writer, sheet_name=key, index=False)
            print(f"Added {key} data to {output_path} in sheet {key}")  # Debug print

    print(f"All merged data saved to {output_path}")

if __name__ == "__main__":
    main()
