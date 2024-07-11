import pandas as pd
import os
from datetime import datetime, timedelta
from openpyxl import load_workbook
from openpyxl.styles import NamedStyle, Font, PatternFill, Alignment

# Define the potential base directories
path_options = [
    '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/',
    '/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data'
]
# Iterate over the list and set base_dir to the first existing path
for path in path_options:
    if os.path.exists(path):
        base_dir = path
        break
else:
    # If no valid path is found, raise an error or handle it appropriately
    print("None of the specified directories exist.")
    base_dir = None  # Or set a default path if appropriate
print("Base directory set to:", base_dir)
static_dir = os.path.join(base_dir, 'Tables')
inventory_file_path = os.path.join(static_dir, 'R_EstoqComp.xlsx')  # Update to the correct path if needed

column_rename_dict = {
    'O_NFCI': {
        'Operação': 'OP',
        'Nota Fiscal': 'NF',
        'Data de Emissão (completa)': 'EMISS',
        'Cliente (Razão Social)': 'NOMERS',
        'Cliente (Nome Fantasia)': 'NOMEF',
        'Código do Produto': 'CODPF', 
        'Quantidade': 'QTD', 
        'Total de Mercadoria': 'MERCVLR',
        'Valor do ICMS ST': 'ICMSST',
        'Valor do IPI': 'IPI',
        'Total da Nota Fiscal': 'TOTALNF',
        'Valor do ICMS': 'ICMS',
        'Estado': 'UF'
 
        # Add other columns that need renaming for O_NFCI
    },
    'B_Estoq': {
        'Código': 'CODPF'
        # Add other columns that need renaming for B_Estoq
    },
    # Add dictionaries for other dataframes...
}

column_format_dict = {
    'O_NFCI': {
        'EMISS': 'DD-MMM-YY',
        'QTD': '0',
        'MERCVLR': '#,##0.00',
        'ICMSST': '#,##0.00',
        'IPI': '#,##0.00',
        'TOTALNF': '#,##0.00',
        'ICMS': '#,##0.00',
        'ECU': '#,##0.00',
        'COMISSPCT': '0.00%',
        'FRETEPCT': '0.00%',
        'VERBAPCT': '0.00%',
        'ECT': '#,##0.00',
        'COMISSVLR': '#,##0.00',
        'FRETEVLR': '#,##0.00',
        'MARGVLR': '#,##0.00',
        'MARGPCT': '0.00%',
        # Add other formats for O_NFCI
    },
    'B_Estoq': {
        'CODPF': '@',
        # Add other formats for B_Estoq
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

def load_recent_data(base_dir, file_pattern, months=1):
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

    # compute column ANOMES
    compute_NFCI_ANOMES(all_data)

    # Merge O_NFCI with T_Remessas - REM
    print(f"Making column REM_NF in O_NFCI")
    all_data = merge_data(all_data, "O_NFCI", "NomeF", "T_Remessas", "NomeF", "REM_NF", default_value=0)

    # Merge O_NFCI with T_Prodf - CODPP
    print(f"Making column CODPP in O_NFCI")
    all_data = merge_data(all_data, "O_NFCI", "CodPF", "T_ProdF", "CodPF", "CODPP", default_value="xxx")

    # Merge O_NFCI with T_GruposCli - G1
    print(f"Making column G1 in O_NFCI")
    all_data = merge_data(all_data, "O_NFCI", "NomeF", "T_GruposCli", "NomeF", "G1", default_value="V")

    # Merge O_NFCI with ECU on columns 'EMISS' and 'CodPF'
    print(f"Making column ECU in O_NFCI")
    all_data = merge_data2v(all_data, "O_NFCI", "ANOMES", "CodPF", "ECU", "ANOMES", "CODPF", "VALUE", "ECU", default_value=0)
 
    # Merge VENDEDOR with T_REPS for COMPCT
    print(f"Making column COMPCT in O_NFCI")
    all_data = merge_data(all_data, "O_NFCI", "Vendedor", "T_Reps", "Vendedor", "COMISSPCT", default_value=0)
#   df.rename(columns={'COMISS': 'COMPCT'}, inplace=True)
#   all_data = merge_data(all_data, "O_NFCI", "VENDEDOR", "T_REPS", "VENDEDOR", "COMPCT", default_value="error")

    # Merge UF with T_Fretes for FretePCT
    print(f"Making column FretePCT in O_NFCI")
    all_data = merge_data(all_data, "O_NFCI", "UF", "T_Fretes", "UF", "FRETEPCT", default_value=0)

    # Merge NomeF with T_Fretes for VerbaPct
    print(f"Making column VerbaPct in O_NFCI")
    all_data = merge_data(all_data, "O_NFCI", "NOMEF", "T_Verbas", "NomeF", "VERBAPCT", default_value=0)
    print(f"Finished making VerbaPCT")

    
    for key, df in all_data.items():
        if key == 'O_NFCI':
            print_table_and_columns(all_data, "O_NFCI")
            # Create column "C"
            df['C'] = 1 - df['REM_NF']
            
            # Create column "B"
            df['B'] = df.apply(lambda row: 1 if row['OP'] == 'REMESSA DE PRODUTO' and row['C'] == 1 else 0, axis=1)
            
            # Create column ECT (ECU x QTD)
            df['ECT'] = df['ECU'] * df['QTD']
 
            # Create column COMVLR (VLRMERC x COMPCT)
            df['COMISSVLR'] = df['MERCVLR'] * df['COMISSPCT']

            # Create column FreteVLR (FretePCT x TotalNF)
            df['FRETEVLR'] = df['FRETEPCT'] * df['TOTALNF']

            # Create column VerbaVLR (VerbaPCT x TotalNF)
            df['VERBAVLR'] = df['VERBAPCT'] * df['TOTALNF']

            # Create column MargCVlr
            df['MARGVLR'] = df['MERCVLR'] * (1 - 0.0925) - df['ICMS'] - df['VERBAVLR'] - df['FRETEVLR'] - df['COMISSVLR'] - df['ECT']

            # Create column VerbaVLR (VerbaPCT x TotalNF)
            df['MARGPCT'] = df['MARGVLR'] / df['MERCVLR']

        # Update the dataframe in all_data
        all_data[key] = df

    return all_data

def preprocess_inventory_data(file_path):
    sheets = pd.read_excel(file_path, sheet_name=None, header=1)  # Load data with headers from the second row
    processed_sheets = {}

    for sheet_name, df in sheets.items():
        df = df.melt(id_vars=['CodPF'], var_name='ANOMES', value_name='Value')
        df['ANOMES'] = pd.to_datetime(df['ANOMES'], errors='coerce').dt.strftime('%y%m')
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
        elif new_col and default_value is not None:
            merged_df[new_col] = merged_df[new_col].fillna(default_value)

        all_data[df1_name] = merged_df
    return all_data

def merge_data2v(all_data, df1_name, df1_col1, df1_col2, df2_name, df2_col1, df2_col2, value_col, new_col_name, default_value=None):
    df1_col1 = df1_col1.upper()
    df1_col2 = df1_col2.upper()
    df2_col1 = df2_col1.upper()
    df2_col2 = df2_col2.upper()
    value_col = value_col.upper()
    new_col_name = new_col_name.upper()

    if df1_name in all_data and df2_name in all_data:
        df1 = all_data[df1_name]
        df2 = all_data[df2_name]

        # Standardize column names
        df1.columns = [col.upper() for col in df1.columns]
        df2.columns = [col.upper() for col in df2.columns]

        print(f"Columns in {df1_name} before merge: {df1.columns}")
        print(f"Columns in {df2_name} before merge: {df2.columns}")

        if df1_col1 not in df1.columns or df1_col2 not in df1.columns or df2_col1 not in df2.columns or df2_col2 not in df2.columns:
            raise KeyError(f"One of the columns '{df1_col1}', '{df1_col2}', '{df2_col1}', '{df2_col2}' not found in dataframes.")

        df2_cols = [df2_col1, df2_col2, value_col]
        merged_df = df1.merge(df2[df2_cols].drop_duplicates(), left_on=[df1_col1, df1_col2], right_on=[df2_col1, df2_col2], how='left')

        if value_col and default_value is not None:
            merged_df[value_col] = merged_df[value_col].fillna(default_value)

        # Rename the value column to the new column name
        merged_df.rename(columns={value_col: new_col_name}, inplace=True)

        print(f"Columns after merge: {merged_df.columns}")
        all_data[df1_name] = merged_df
    return all_data


def compute_NFCI_ANOMES(all_data):
    for key, df in all_data.items():
        # Add the ANOMES column to O_NFCI
        if key == 'O_NFCI' and 'EMISS' in df.columns:
            df['EMISS'] = pd.to_datetime(df['EMISS'], errors='coerce')  # Ensure the date is parsed correctly
            df['ANOMES'] = df['EMISS'].dt.strftime('%y%m')  # Format date as YYMM
            print(f"Added ANOMES column to {key}")
        all_data[key] = df
    return all_data

def load_inventory_data(file_path):
    return pd.read_excel(file_path)

def print_all_tables_and_columns(all_data):
    for table_name, df in all_data.items():
        print(f"Table: {table_name}")
        print("Columns:", df.columns.tolist())
        print("-" * 50)

def print_table_and_columns(all_data, table_name):
    if table_name in all_data:
        print(f"Table: {table_name}")
        print("Columns:", all_data[table_name].columns.tolist())
        print("-" * 50)
    else:
        print(f"Table '{table_name}' not found in the dataset.")

def excel_format(output_path, column_format_dict):
    print("Formatting all sheets")
    header_style = NamedStyle(name="header_style")
    header_style.font = Font(bold=True)
    header_style.fill = PatternFill("solid", fgColor="6ac5fe")  # Light blue background color
    header_style.alignment = Alignment(horizontal="center", vertical="center")

    workbook = load_workbook(output_path)
    for sheet_name in workbook.sheetnames:
        worksheet = workbook[sheet_name]
        
        # Apply header style
        for col_idx in range(1, worksheet.max_column + 1):
            cell = worksheet.cell(row=1, column=col_idx)
            cell.style = header_style

        if sheet_name in column_format_dict:
            formats = column_format_dict[sheet_name]
            for col_name, col_format in formats.items():
                # Find the column index based on header name
                for col_idx, cell in enumerate(worksheet[1], start=1):
                    if cell.value == col_name:
                        break
                else:
                    continue  # Skip if column name is not found

                # Apply the format to the entire column
                for row_idx in range(2, worksheet.max_row + 1):  # Start from the second row to avoid header
                    cell = worksheet.cell(row=row_idx, column=col_idx)
                    cell.number_format = col_format

    workbook.save(output_path)
    print(f"All sheets formatted")


def excel_autofilters(output_path):
    print("Adding auto-filters to all sheets")
    workbook = load_workbook(output_path)
    for sheetname in workbook.sheetnames:
        worksheet = workbook[sheetname]
        worksheet.auto_filter.ref = worksheet.dimensions
    workbook.save(output_path)
    print("Added auto-filters to all sheets")

def main():
    #base_dir = '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/'
    #static_dir = os.path.join(base_dir, 'Tables')
    #inventory_file_path = os.path.join(static_dir, 'R_EstoqComp.xlsx')  # Update to the correct path if needed

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
                     'T_RegrasMP.xlsx', 'T_Remessas.xlsx', 'T_Reps.xlsx', 'T_Verbas.xlsx','T_Vol.xlsx', 'T_ProdF.xlsx', 'T_ProdP.xlsx', 'T_Entradas.xlsx']
    static_data_dict = {table.replace('.xlsx', ''): load_static_data(static_dir, table) for table in static_tables}
    
    # Check static data shapes
    for key, df in static_data_dict.items():
        print(f"Static data {key} shape: {df.shape}")  # Debug print
    
    inventory_data = preprocess_inventory_data(inventory_file_path)
 
    # Add static data to all_data dictionary
    all_data.update(static_data_dict) 
    all_data.update(inventory_data)
    
    all_data = rename_columns(all_data, column_rename_dict)

    # Merge all data with static data
    all_data = merge_all_data(all_data) 

    # Save all data to one Excel file with multiple sheets
    output_path = os.path.join(base_dir, 'clean', 'merged_data.xlsx')
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for key, df in all_data.items():
            df.to_excel(writer, sheet_name=key, index=False)
            print(f"Added {key} data to {output_path} in sheet {key}")  # Debug print

    print(f"All merged data saved to {output_path}")

    excel_format(output_path, column_format_dict)
    excel_autofilters(output_path)

if __name__ == "__main__":
    main()