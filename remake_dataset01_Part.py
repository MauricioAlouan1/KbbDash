"""
This script, `remake_dataset.py`, is part of the KBB MF data pipeline for processing financial and inventory datasets. 
Its primary objectives include:
1. Setting up directories for dynamic and static data sources.
2. Importing essential libraries such as pandas and openpyxl for data manipulation and file handling.
3. Processing and merging datasets, including:
   - Combining dynamic data files with static lookup tables stored in the "Tables" directory.
   - Cleaning and transforming data for reporting and dashboard creation.
4. Leveraging utility functions for tasks like locating directories, managing dates, and formatting Excel outputs.

Prerequisites:
- Ensure that the directories specified in `path_options` exist and contain the necessary files.
- Verify that the "Tables" directory holds the required static tables for lookup and merging operations.

This script is integral to maintaining data accuracy and efficiency in the reporting workflow.
"""


import pandas as pd
from pandas.tseries.offsets import MonthEnd
import os
import shutil
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import NamedStyle, Font, PatternFill, Alignment, Border, Side
import re

#Global
ano_x = 2025
mes_x = 9

# Format month as two digits (01, 02, ..., 12)
mes_str = f"{mes_x:02d}"
ano_mes = f"{ano_x}_{mes_str}"  # e.g., "2025_01"

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
#inventory_file_path = os.path.join(static_dir, 'R_EstoqComp.xlsx')  # Update to the correct path if needed
template_file = os.path.join(base_dir, "Template", "PivotTemplate.xlsm")
output_file = os.path.join(base_dir, "clean", ano_mes, f"R_Resumo_{ano_mes}.xlsm")

# Step 1: Copy the template (preserves macros)
shutil.copy(template_file, output_file)
print(f"✅ Copied template to {output_file}")

# Step 2: Open the template workbook with macros
print("✅ Opening template with macros...")
wb_template = load_workbook(output_file, keep_vba=True)

# Step 3: Remove all existing sheets from the template
print(f"✅ Removing {len(wb_template.sheetnames)} sheets from template...")
for sheet in wb_template.sheetnames:
    del wb_template[sheet]

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
     'L_LPI': {
        'Preço Com Desconto': 'VLRVENDA',
        'SKU': 'CODPF',
        'Vendas': 'QTD'
    },
    'MLK_Vendas' : {
        'PREÇO UNITÁRIO DE VENDA DO ANÚNCIO (BRL)': 'PRECOUNIT',
        'Quantidade' : 'QTD',
        'DATA DA VENDA' : 'DATA'
    }
    # Add dictionaries for other dataframes...
}
column_format_dict = {
    'O_NFCI': {
        'EMISS': 'DD-MMM-YY',
        'QTD': '0',
        'PRECO CALC': '#,##0.00',
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
    'L_LPI':{
        'VLRVENDA': '#,##0.00',        
        'DESCONTO PEDIDO SELLER': '#,##0.00',        
        'FRETE SELLER': '#,##0.00',        
        'ECUK': '#,##0.00',
        'ECTK': '#,##0.00',
        'ComissPctMp': '0.0%',
        'ComissPctVlr': '#,##0.00',
        'FreteFixoVlr': '#,##0.00',
        'FreteProdVlr': '#,##0.00',
        'REPASSE': '#,##0.00',
        'ImpLP': '#,##0.00',
        'ImpICMS': '#,##0.00',
        'ImpTot': '#,##0.00',
        'MargVlr': '#,##0.00',
        'MargPct': '0.0%',
    },
    'MLK_Vendas':{
        'VLRTOTALPSKU': '#,##0.00',
        'RECEITAENVIO': '#,##0.00',
        'TARIFAVENDA': '#,##0.00',
        'TARIFAENVIO': '#,##0.00',
        'CANCELAMENTOS': '#,##0.00',
        'REPASSE': '#,##0.00',
        'ECU': '#,##0.00',
        'ECTK': '#,##0.00',
        'Imposto1': '#,##0.00',
        'Imposto2': '#,##0.00',
        'ImpostoT': '#,##0.00',
        'MARGVLR': '#,##0.00',
        'MARGPCT': '0.00%',
    },
    'O_CC': {
        'DATA': 'DD-MMM-YY',  
        'VALOR (R$)': '#,##0.00',
    },
    'O_CtasAPagar': {
        'PREVISÃO': 'DD-MMM-YY',  
        'EMISSÃO': 'DD-MMM-YY',  
        'VENCIMENTO': 'DD-MMM-YY',  
        'REGISTRO': 'DD-MMM-YY',  
        'A PAGAR': '#,##0.00',
    },
    'O_CtasARec': {
        'PREVISÃO': 'DD-MMM-YY',  
        'EMISSÃO': 'DD-MMM-YY',  
        'VENCIMENTO': 'DD-MMM-YY',  
        'REGISTRO': 'DD-MMM-YY',  
        'A RECEBER': '#,##0.00',
        'DATA BASE': 'DD-MMM-YY',  
    },
}
rows_todrop = {
    'O_NFCI': {
        'C': 0,
    }
}
cols_todrop = {
    'O_NFCI': {
        'PROJETO': 'd',
        'C': 'd',
    },
    'MLK_Vendas':{
        'RECEITA POR ACRÉSCIMO NO PREÇO (PAGO PELO COMPRADOR)': 'd',
        'TAXA DE PARCELAMENTO EQUIVALENTE AO ACRÉSCIMO': 'd',
        'ENDEREÇO': 'd',
        'ENDEREÇO01': 'd',
        'CIDADE': 'd',
        'ESTADO': 'd',
        'CEP': 'd',
    },
    'O_CC': {
        'SALDO (R$)': 'd',
    },
    'O_CtasAPagar': {
        'MINHA EMPRESA (NOME FANTASIA)': 'd',
        'MINHA EMPRESA (RAZÃO SOCIAL)': 'd',
        'MINHA EMPRESA (CNPJ)': 'd',
        'ORÇAMENTO': 'd',
        'VALOR DA CONTA': 'd',
        'VALOR PIS': 'd',
        'VALOR COFINS': 'd',
        'VALOR CSLL': 'd',
        'VALOR IR': 'd',
        'VALOR ISS': 'd',
        'VALOR INSS': 'd',
        'VALOR LÍQUIDO': 'd',
        'VALOR PAGO': 'd',
    },
    'O_CtasARec': {
        'MINHA EMPRESA (NOME FANTASIA)': 'd',
        'MINHA EMPRESA (RAZÃO SOCIAL)': 'd',
        'MINHA EMPRESA (CNPJ)': 'd',
        'ORÇAMENTO': 'd',
        'VALOR DA CONTA': 'd',
        'VALOR PIS': 'd',
        'VALOR COFINS': 'd',
        'VALOR CSLL': 'd',
        'VALOR IR': 'd',
        'VALOR ISS': 'd',
        'VALOR INSS': 'd',
        'VALOR LÍQUIDO': 'd',
        'RECEBIDO': 'd',
    },

}

audit_client_names = ['ALWE', 'COMPROU CHEGOU', 'NEXT COMPRA']  # Add other clients as needed
invaudit_client_names = ['ALWE', 'COMPROU CHEGOU', 'NEXT COMPRA']  # Add other clients as needed

def rename_columns(all_data, column_rename_dict):
    for df_name, rename_dict in column_rename_dict.items():
        if df_name in all_data:
            df = all_data[df_name]
            # Debug print to verify columns before renaming
            #print(f"Before rename:\nTable: {df_name}\nColumns: {df.columns.tolist()}")
            df.rename(columns=rename_dict, inplace=True)
            # Debug print to verify columns after renaming
            #print(f"Renamed columns in {df_name}: {rename_dict}")
            #print(f"Columns in {df_name}: {df.columns.tolist()}")
            all_data[df_name] = df
    return all_data

def load_recent_data(base_dir, file_pattern, ds_year = ano_x, ds_month = mes_x):
 
    frames = []
    current_date = datetime(ds_year, ds_month, 1)
    year_month = current_date.strftime('%Y_%m')
    file_path = os.path.join(base_dir, 'clean', year_month, file_pattern.format(year_month=year_month))

    # Specify which columns should always be treated as strings
    string_columns = ["ORDER_ID", "TRANSACTION_ID", "SHIPPING_ID", "SOURCE_ID", "EXTERNAL_REFERENCE"]

    if os.path.exists(file_path):
        df = pd.read_excel(file_path, dtype={col: str for col in string_columns})
        frames.append(df)
        #print(f"Loaded {file_path} with shape: {df.shape}")  # Debug print
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

def clean_dataframes(all_data):
    """
    Drops specified rows and columns from each table in all_data based on global variables rows_todrop and cols_todrop.
    
    Parameters:
    - all_data (dict): Dictionary containing all datasets.

    Returns:
    - all_data (dict): Updated dictionary with rows and columns removed as per defined rules.
    """
    global rows_todrop, cols_todrop

    for key, df in all_data.items():
        # Drop rows based on conditions in rows_todrop
        if key in rows_todrop:
            for col, value in rows_todrop[key].items():
                if col in df.columns:
                    df = df[df[col] != value]  # Keep rows where column is NOT equal to the specified value
                    print(f"Dropped rows in {key} where {col} = {value}")

        # Drop columns based on conditions in cols_todrop
        if key in cols_todrop:
            cols_to_remove = [col for col in cols_todrop[key].keys() if col in df.columns]
            df = df.drop(columns=cols_to_remove, errors='ignore')
            print(f"Dropped columns {cols_to_remove} from {key}")

        # Update the dataframe in all_data
        all_data[key] = df

    return all_data

def merge_all_data(all_data):
    print(f"Creating Merged and Calculated Columns")

    # Ensure all relevant columns are in uppercase for case-insensitive comparison
    all_data = {key: standardize_text_case(df) for key, df in all_data.items()}

    # compute column ANOMES
    compute_NFCI_ANOMES(all_data)
    compute_LPI_ANOMES(all_data)
    compute_ML_ANOMES(all_data)
    compute_CC_ANOMES(all_data)

    # Merge O_NFCI with T_Remessas - REM
    all_data = merge_data(all_data, "O_NFCI", "NOMEF", "T_Remessas", "NOMEF", "REM_NF", default_value=0)

    # Merge O_NFCI with T_Prodf - CODPP
    all_data = merge_data(all_data, "O_NFCI", "CodPF", "T_ProdF", "CodPF", "CODPP", default_value="xxx")
    all_data = merge_data(all_data, "L_LPI", "CodPF", "T_ProdF", "CodPF", "CODPP", default_value="xxx")
    all_data = merge_data(all_data, "MLA_Vendas", "SKU", "T_ProdF", "CodPF", "CODPP", default_value="xxx")
    all_data = merge_data(all_data, "MLK_Vendas", "SKU", "T_ProdF", "CodPF", "CODPP", default_value="xxx")

    # Merge O_NFCI with T_GruposCli - G1
    all_data = merge_data(all_data, "O_NFCI", "NomeF", "T_GruposCli", "NomeF", "G1", default_value="V")

    # Merge O_NFCI with ECU on columns 'EMISS' and 'CodPF'
    all_data = merge_data_lastcost(all_data, df1_name="O_NFCI",        # Main sales table
        df1_product_col="CODPP",  # Product code in main table
        df1_date_col="EMISS",     # Sale date column
        df2_name="T_Entradas",    # Cost data table
        df2_product_col="PAI",    # Product code in cost table
        df2_date_col="ULTIMA ENTRADA",  # Purchase date column
        df2_cost_col="ULT CU R$",       # Cost column
        new_col_name="ECU",     # New column name for retrieved cost
    default_value=999           # Default cost if no match is found
)
        # Merge O_NFCI with ECU on columns 'EMISS' and 'CodPF'
    all_data = merge_data_lastcost(all_data, df1_name="L_LPI",        # Main sales table
        df1_product_col="CODPP",  # Product code in main table
        df1_date_col="DATA",     # Sale date column
        df2_name="T_Entradas",    # Cost data table
        df2_product_col="PAI",    # Product code in cost table
        df2_date_col="ULTIMA ENTRADA",  # Purchase date column
        df2_cost_col="ULT CU R$",       # Cost column
        new_col_name="ECUK",     # New column name for retrieved cost
    default_value=999           # Default cost if no match is found
)
        # Merge O_NFCI with ECU on columns 'EMISS' and 'CodPF'
    all_data = merge_data_lastcost(all_data, df1_name="MLK_Vendas",        # Main sales table
        df1_product_col="CODPP",  # Product code in main table
        df1_date_col="DATA DA VENDA",     # Sale date column
        df2_name="T_Entradas",    # Cost data table
        df2_product_col="PAI",    # Product code in cost table
        df2_date_col="ULTIMA ENTRADA",  # Purchase date column
        df2_cost_col="ULT CU R$",       # Cost column
        new_col_name="ECU",     # New column name for retrieved cost
    default_value=999           # Default cost if no match is found
)

    all_data = merge_data2v(all_data, "MLA_Vendas", "ANOMES", "SKU", "ECU", "ANOMES", "CODPF", "VALUE", "ECU", default_value=999)
 
    # Merge VENDEDOR with T_REPS for COMPCT
    all_data = merge_data(all_data, "O_NFCI", "Vendedor", "T_Reps", "Vendedor", "COMISSPCT", default_value=0)

    # Merge UF with T_Fretes for FretePCT
    all_data = merge_data(all_data, "O_NFCI", "UF", "T_Fretes", "UF", "FRETEPCT", default_value=0)
    # Set FRETEPCT = 0 where G1 = "DROP" or "ALWE" in O_NFCI table
    if 'O_NFCI' in all_data:
        all_data['O_NFCI'].loc[all_data['O_NFCI']['G1'].isin(['DROP', 'ALWE']), 'FRETEPCT'] = 0

    # Merge NomeF with T_Verbas for VerbaPct
    all_data = merge_data(all_data, "O_NFCI", "NOMEF", "T_Verbas", "NomeF", "VERBAPCT", default_value=0)

    # Perform the merge (example merge, adjust as necessary)
    all_data = merge_data(all_data, "L_LPI", "INTEGRAÇÃO", "T_MP", "Integração", "Empresa", default_value='erro')
    all_data = merge_data(all_data, "L_LPI", "INTEGRAÇÃO", "T_MP", "Integração", "MP", default_value='erro')
    all_data = merge_data(all_data, "L_LPI", "INTEGRAÇÃO", "T_MP", "Integração", "EmpresaF", default_value='erro')

    # OrderStatus Merge
    all_data = merge_data(all_data, "MLA_Vendas", "STATUS", "T_MLStatus", "MLStatus", "OrderStatus", default_value='erro')
    all_data = merge_data(all_data, "MLK_Vendas", "STATUS", "T_MLStatus", "MLStatus", "OrderStatus", default_value='erro')

    # Ctas a Pagar e Receber
    all_data = merge_data(all_data, "O_CtasAPagar", "CATEGORIA", "T_CtasAPagarClass", "Categoria", "GrupoCtasAPagar", default_value='erro')
    #all_data = merge_data(all_data, "O_CtasARec", "CATEGORIA", "T_CtasARecClass", "Categoria", "GrupoCtasAPagar", default_value='erro')

    # CC
    all_data = merge_data(all_data, "O_CC", "CATEGORIA", "T_CCCats", "CC_Categoria Omie", "CC_Cat SG", default_value='erro')
    all_data = merge_data(all_data, "O_CC", "CATEGORIA", "T_CCCats", "CC_Categoria Omie", "CC_Cat Grp", default_value='erro')
    all_data = merge_data(all_data, "O_CC", "CATEGORIA", "T_CCCats", "CC_Categoria Omie", "CC_B2X", default_value='erro')
    all_data = merge_data(all_data, "O_CC", "CATEGORIA", "T_CCCats", "CC_Categoria Omie", "CC_Tipo", default_value='erro')
    
    for key, df in all_data.items():
        if key == 'O_NFCI':
            # print_table_and_columns(all_data, "O_NFCI")

            # Create column "C" (C)onta pra calculo (remessa não conta)
            df['C'] = 1 - df['REM_NF']
            
            # Create column "B" (B)onificado
            df['B'] = df.apply(lambda row: 1 if row['OP'] == 'REMESSA DE PRODUTO' and row['C'] == 1 else 0, axis=1)
            
            # Create column ECT (ECU x QTD)
            df['ECT'] = df['ECU'] * df['QTD'] * df['C']
 
            # Create column COMVLR (VLRMERC x COMPCT)
            df['COMISSVLR'] = df['MERCVLR'] * df['COMISSPCT'] * df['C']

            # Create column FreteVLR (FretePCT x TotalNF)            
            #df['FRETEVLR'] = df['FRETEPCT'] * df['TOTALNF'] * df['C']
            df['FRETEVLR'] = df.apply(lambda row: max(row['FRETEPCT'] * row['TOTALNF'] * row['C'], row['FRETEPCT'] * row['ECT'] * row['C'] * 2), axis=1)

            # Create column VerbaVLR (VerbaPCT x TotalNF)
            df['VERBAVLR'] = df['VERBAPCT'] * df['TOTALNF'] * df['C']

            # Create column MargCVlr
            df['MARGVLR'] = df['C'] * ( df['MERCVLR'] * (1 - 0.0925) - df['ICMS'] ) - df['VERBAVLR'] - df['FRETEVLR'] - df['COMISSVLR'] - df['ECT']

            # Create column VerbaVLR (VerbaPCT x TotalNF)
            df['MARGPCT'] = df['MARGVLR'] / df['MERCVLR']

        elif key == 'L_LPI':
            cols_to_drop = ['PREÇO', 'PREÇO TOTAL', 'DESCONTO ITEM', 'DESCONTO TOTAL']
            df = df.drop([x for x in cols_to_drop if x in df.columns], axis=1)
            # Add the 'Valido' column directly
            df["MP2"] = df["MP"].str[:2]
            df['VALIDO'] = df['STATUS PEDIDO'].apply(lambda x: 0 if x in ['CANCELADO', 'PENDENTE', 'AGUARDANDO PAGAMENTO'] else 1)
            df['KAB'] = df.apply(lambda row: 1 if row['VALIDO'] == 1 and row['EMPRESA'] in ['K', 'A', 'B'] else 0, axis=1)
            #print("#### DEBUG  ####")
            #print("Unique values in EMPRESA:", df['EMPRESA'].unique())
            #print("Unique values in VALIDO:", df['VALIDO'].unique())
            #print("Unique values in KAB:", df['KAB'].unique())

            df['ECTK'] = df['ECUK'] * df['QTD'] * df['KAB']

            # Add the 'TipoAnuncio' column directly from 'MLK_Vendas'
            if 'MLK_Vendas' in all_data:
                print_table_head(all_data, "MLK_Vendas")
                df = df.merge(
                    all_data['MLK_Vendas'][['N.º DE VENDA', 'TIPO DE ANÚNCIO']],
                    left_on='CÓDIGO PEDIDO',
                    right_on='N.º DE VENDA',
                    how='left'
                )
                df['TipoAnuncioK'] = df.apply(lambda row: 'ML' + row['TIPO DE ANÚNCIO'][:2] if pd.notna(row['TIPO DE ANÚNCIO']) and row['EMPRESA'] == 'K' and row['MP'] == 'ML' else None, axis=1)                    
                df.drop(columns=['N.º DE VENDA', 'TIPO DE ANÚNCIO'], inplace=True)

            # Add the 'TipoAnuncio' column for 'ALWE' and lookup in 'MLA_Vendas'
            if 'MLA_Vendas' in all_data and not all_data['MLA_Vendas'].empty:
                df = df.merge(
                    all_data['MLA_Vendas'][['N.º DE VENDA', 'TIPO DE ANÚNCIO']],
                    left_on='CÓDIGO PEDIDO',
                    right_on='N.º DE VENDA',
                    how='left'
                )
                df['TipoAnuncioA'] = df.apply(lambda row: 'ML' + row['TIPO DE ANÚNCIO'][:2] if pd.notna(row['TIPO DE ANÚNCIO']) and row['EMPRESA'] == 'A' and row['MP'] == 'ML' else None, axis=1)                    
                df.drop(columns=['N.º DE VENDA', 'TIPO DE ANÚNCIO'], inplace=True)

            # Add the 'TipoAnuncio' column for 'Baby Trends' and lookup in 'MLB_Vendas'
            if 'MLB_Vendas' in all_data:
                df = df.merge(
                    all_data['MLA_Vendas'][['N.º DE VENDA', 'TIPO DE ANÚNCIO']],
                    left_on='CÓDIGO PEDIDO',
                    right_on='N.º DE VENDA',
                    how='left'
                )
                df['TipoAnuncioB'] = df.apply(lambda row: 'ML' + row['TIPO DE ANÚNCIO'][:2] if pd.notna(row['TIPO DE ANÚNCIO']) and row['EMPRESA'] == 'B' and row['MP'] == 'ML' else None, axis=1)                    
                df.drop(columns=['N.º DE VENDA', 'TIPO DE ANÚNCIO'], inplace=True)
            else:
                # If 'MLB_Vendas' is not in all_data, create 'TipoAnuncioB' and fill with 'G' for EMPRESA == 'B' and MP == 'ML'
                df['TipoAnuncioB'] = df.apply(lambda row: 'MLG' if row['EMPRESA'] == 'B' and row['MP'] == 'ML' else None, axis=1)

            # Merge Tipo de AnnuncioK/A/B into MP. Ensure that we only process rows where MP == 'ML'
            df['MP'] = df.apply(
                lambda row: (
                    row.get('TipoAnuncioK', None)
                    if row.get('EMPRESA') == 'K' and row.get('MP') == 'ML' and pd.notna(row.get('TipoAnuncioK', None)) else
                    row.get('TipoAnuncioA', None)
                    if row.get('EMPRESA') == 'A' and row.get('MP') == 'ML' and pd.notna(row.get('TipoAnuncioA', None)) else
                    row.get('TipoAnuncioB', None)
                    if row.get('EMPRESA') == 'B' and row.get('MP') == 'ML' and pd.notna(row.get('TipoAnuncioB', None)) else
                    row.get('MP')
                ),
                axis=1
            )

            # Drop the now redundant columns
            df.drop(columns=['TipoAnuncioK', 'TipoAnuncioA', 'TipoAnuncioB'], inplace=True, errors="ignore")

            # Add column Compctmp (Comissão pct por Marketplace)
            if 'T_RegrasMP' in all_data:
                df = df.merge(
                    all_data['T_RegrasMP'][['MPX', 'TARMP', 'FFABAIXODE', 'FRETEFIX']],
                    left_on='MP',
                    right_on='MPX',
                    how='left'
                )
                # Assign marketplace commission percentage
                df['ComissPctMp'] = df['TARMP']

                # Create the ComPct column based on the condition
                df['ComissPctVlr'] = df['VLRVENDA'] * df['ComissPctMp'] * -1

                # Assign fixed shipping fee based on VLRVENDA threshold
                df['FreteFixoVlr'] = df.apply(
                    lambda row: -row['FRETEFIX'] if row['VLRVENDA'] < row['FFABAIXODE'] else 0,
                    axis=1
                )
                # Drop unnecessary columns
                df.drop(columns=['MPX', 'TARMP', 'FFABAIXODE', 'FRETEFIX'], inplace=True)

            # Compute FretePaiVlr based on T_FretesMP
            if 'T_FretesMP' in all_data:
                df_fretesmp = all_data['T_FretesMP']

                # Extract first two letters of MP
                df['MP_2L'] = df['MP'].str[:2].str.upper()

                # Function to lookup the freight cost
                def get_frete_pai(row):
                    codpp = row['CODPP']
                    mp_col = row['MP_2L']

                    # Ensure the column exists in T_FretesMP
                    if mp_col not in df_fretesmp.columns:
                        return 99  # If marketplace column doesn't exist, return 99

                    # Try to find freight cost for the given CODPP
                    match = df_fretesmp[df_fretesmp['CODPP'] == codpp]
                    if not match.empty:
                        return match[mp_col].values[0]  # Return corresponding marketplace freight cost

                    # If CODPP not found, use generic cost for 'XXX'
                    generic_match = df_fretesmp[df_fretesmp['CODPP'] == 'XXX']
                    if not generic_match.empty:
                        return generic_match[mp_col].values[0]

                    return 99  # Default to 0 if no match found

                # Apply lookup function
                df['FreteProdVlr'] = df.apply(lambda row: -get_frete_pai(row), axis=1)

                # Drop temporary column MP_2L
                df.drop(columns=['MP_2L'], inplace=True)

            # Create column Rebate for later use
            df['Rebate'] = 0.0
            df['REPASSE'] = df['VLRVENDA'] + df['ComissPctVlr'] + df['FreteFixoVlr'] + df['FreteProdVlr'] + df['Rebate']
            df['ImpLP'] = df.apply(
                lambda row: -0.0925 * row['VLRVENDA'] if row['EMPRESA'] == 'K' else
                            -0.14 * row['VLRVENDA'] if row['EMPRESA'] == 'A' else
                            -0.10 * row['VLRVENDA'] if row['EMPRESA'] == 'B' else 0,
                axis=1)
            df['ImpICMS'] = df.apply(
                lambda row: -0.18 * row['VLRVENDA'] if row['EMPRESA'] == 'K' else 0,
                axis=1)
            df['ImpTot'] = df['ImpLP'] + df['ImpICMS']

            df['MargVlr'] = df.apply(
                lambda row: 0 if row['EMPRESA'] == 'NC' else
                            row['REPASSE'] + row['ImpTot'] - row['ECTK'] - 1 - (0.01)*row['VLRVENDA'] if row['EMPRESA'] == 'K' else
                            row['REPASSE'] + row['ImpTot'] - 1.6 * row['ECTK'],
                axis=1)

            # Create column VerbaVLR (VerbaPCT x TotalNF)
            df['MargPct'] = df['MargVlr'] / df['VLRVENDA']

        elif key == 'MLA_Vendas':
            if not df.empty and 'VLRTOTALPSKU' in df.columns:
                # Add the 'VALIDO' column directly
                df['Imposto1'] = df['VLRTOTALPSKU'] * 0.11
                df['Imposto2'] = 0
                df['ImpostoT'] = df['Imposto1'] + df['Imposto2']

                cols_to_drop = ['CODPF_x', 'CODPF_y', 'MLSTATUS']
                df = df.drop([x for x in cols_to_drop if x in df.columns], axis=1)
            else:
                print("⚠️ Skipping MLA_Vendas tax calculation: table missing or incomplete")

        elif key == 'MLK_Vendas':
            # Create column ECT (ECU x QTD)
            df['ECTK'] = df['ECU'] * df['QTD']

            # Add the 'Impostos' columns directly
            df['Imposto1'] = df['VLRTOTALPSKU']*(0.0925)
            df['Imposto2'] = df['VLRTOTALPSKU']*(0.18)
            df['ImpostoT'] =  df['Imposto1'] + df['Imposto2']

            # Create column MargCVlr
            df['MARGVLR'] = df['REPASSE'] - df['ImpostoT'] - df['ECTK'] - (1) -(.01)*df['VLRTOTALPSKU']
            df['MARGPCT'] = df['MARGVLR'] / df['VLRTOTALPSKU']

            cols_to_drop = ['CODPF_x', 'CODPF_y', 'MLSTATUS']
            df = df.drop([x for x in cols_to_drop if x in df.columns], axis=1)

        elif key == 'O_CtasARec':
            if not df.empty and 'ANOMES' in df.columns and 'VENCIMENTO' in df.columns:
                try:
                    # Step 2: Create the 'DATA BASE' column which is the last day of the month
                    df['DATA BASE'] = pd.to_datetime(df['ANOMES'], format='%y%m', errors='coerce') + MonthEnd(0)

                    # Step 3: Calculate 'DIAS ATRASO'
                    df['DIAS ATRASO'] = (df['DATA BASE'] - df['VENCIMENTO']).dt.days

                    # Step 4: Ensure no negatives
                    df['DIAS ATRASO'] = df['DIAS ATRASO'].apply(lambda x: max(0, x) if pd.notna(x) else None)

                    # Step 5: Try classification
                    if 'T_CtasARecClass' in all_data and not all_data['T_CtasARecClass'].empty:
                        df_ctas_a_rec_class = all_data['T_CtasARecClass']

                        def classify_dias_atraso(row):
                            match = df_ctas_a_rec_class[
                                (df_ctas_a_rec_class['DEXDIAS'] <= row['DIAS ATRASO']) &
                                (row['DIAS ATRASO'] <= df_ctas_a_rec_class['ATEXDIAS'])
                            ]
                            return match['STATUS ATRASO'].iloc[0] if not match.empty else None

                        df['CLASSIFICACAO'] = df.apply(classify_dias_atraso, axis=1)
                    else:
                        print("⚠️ Skipping O_CtasARec classification: T_CtasARecClass missing")
                except Exception as e:
                    print(f"⚠️ Skipping O_CtasARec processing due to error: {e}")
            else:
                print("⚠️ Skipping O_CtasARec: table missing or incomplete")

        df = clean_dataframes({key: df})[key]  # Pass only the relevant table for modification
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

    # Verifica se os dataframes existem
    if df1_name not in all_data or df2_name not in all_data:
        print(f"❌ Dataframe '{df1_name}' or '{df2_name}' not found in all_data.")
        return all_data

    df1 = all_data[df1_name]
    df2 = all_data[df2_name]

    # Verifica se os dataframes não estão vazios
    if df1.empty or df2.empty:
        print(f"❌ Dataframe '{df1_name}' or '{df2_name}' is empty. Merge skipped.")
        return all_data

    # Verifica se as colunas existem
    if df1_col not in df1.columns or df2_col not in df2.columns:
        print(f"❌ Column '{df1_col}' or '{df2_col}' not found in dataframes '{df1_name}' or '{df2_name}'. Merge skipped.")
        return all_data

    print(f"Columns in {df1_name} BEFORE merge: {df1.columns.tolist()}")

    cols_to_drop = []
    if new_col and new_col in df1.columns:
        cols_to_drop.append(new_col)
    # Só remover df2_col se NÃO for igual ao campo de junção (df1_col)
    if df2_col != df1_col and df2_col in df1.columns:
        cols_to_drop.append(df2_col)

    if cols_to_drop:
        print(f"⚠️  Dropping existing columns {cols_to_drop} from '{df1_name}' before merge.")
        all_data[df1_name] = all_data[df1_name].drop(columns=cols_to_drop)
        df1 = all_data[df1_name]

    # Faz o merge
    merged = df1.merge(
        df2[[df2_col] + ([new_col] if new_col else [])],
        left_on=df1_col,
        right_on=df2_col,
        how='left',
        indicator=indicator_name is not None
    )

    # Adiciona coluna com valor default se merge falhar
    if new_col and default_value is not None:
        merged[new_col] = merged[new_col].fillna(default_value)

    if indicator_name:
        merged[indicator_name] = merged[indicator_name].fillna('no_match')

    all_data[df1_name] = merged
    print(f"✅ Merge applied: {df1_name} ← {df2_name} by '{df1_col}'")
    return all_data

def merge_data2v(all_data, df1_name, df1_col1, df1_col2, df2_name, df2_col1, df2_col2, df2_val_col, new_col_name, default_value=None, negative=False):
    df1_col1 = df1_col1.upper()
    df1_col2 = df1_col2.upper()
    df2_col1 = df2_col1.upper()
    df2_col2 = df2_col2.upper()
    df2_val_col = df2_val_col.upper()
    new_col_name = new_col_name.upper()

    if df1_name in all_data and df2_name in all_data:
        df1 = all_data[df1_name]
        df2 = all_data[df2_name]

        # Standardize column names
        df1.columns = [col.upper() for col in df1.columns]
        df2.columns = [col.upper() for col in df2.columns]

        #print(f"Columns in {df1_name} before merge: {df1.columns}")
        #print(f"Columns in {df2_name} before merge: {df2.columns}")

        if df1_col1 not in df1.columns or df1_col2 not in df1.columns or df2_col1 not in df2.columns or df2_col2 not in df2.columns:
            raise KeyError(f"One of the columns '{df1_col1}', '{df1_col2}', '{df2_col1}', '{df2_col2}' not found in dataframes.")

        if negative:
            df2[df2_val_col] = df2[df2_val_col] * -1  # Make the VALUE column negative

        df2_cols = [df2_col1, df2_col2, df2_val_col]

        # Remove colunas que já existam com o mesmo nome da futura 'new_col_name'
        cols_to_drop = [col for col in df1.columns if col.upper().startswith(new_col_name)]
        if cols_to_drop:
            print(f"⚠️  Dropping existing columns {cols_to_drop} from '{df1_name}' before merge_data2v.")
            df1 = df1.drop(columns=cols_to_drop)

        merged_df = df1.merge(df2[df2_cols].drop_duplicates(), left_on=[df1_col1, df1_col2], right_on=[df2_col1, df2_col2], how='left')

        if df2_val_col and default_value is not None:
            merged_df[df2_val_col] = merged_df[df2_val_col].fillna(default_value)

        # Rename the value column to the new column name
        merged_df.rename(columns={df2_val_col: new_col_name}, inplace=True)

        #print(f"Columns after merge: {merged_df.columns}")
        all_data[df1_name] = merged_df
    return all_data

def merge_data_lastcost(all_data, df1_name, df1_product_col, df1_date_col, df2_name, df2_product_col, df2_date_col, df2_cost_col, new_col_name, default_value=None):
    """
    Merge df1 (sales table) with df2 (cost table) to get the last recorded cost before the sale date.

    Parameters:
    - all_data (dict): Dictionary containing all datasets.
    - df1_name (str): Key for the main table (sales) in all_data.
    - df1_product_col (str): Column name for the product code in the main table.
    - df1_date_col (str): Column name for the sale date in the main table.
    - df2_name (str): Key for the cost table in all_data.
    - df2_product_col (str): Column name for the product code in the cost table.
    - df2_date_col (str): Column name for the purchase date in the cost table.
    - df2_cost_col (str): Column name for the cost value in the cost table.
    - new_col_name (str): Name of the new column to store the retrieved cost.
    - default_value (optional): Default value to use if no match is found.

    Returns:
    - all_data (dict): Updated dictionary with the main table modified to include last cost.
    """

    # Standardize column names to uppercase
    df1_product_col = df1_product_col.upper()
    df1_date_col = df1_date_col.upper()
    df2_product_col = df2_product_col.upper()
    df2_date_col = df2_date_col.upper()
    df2_cost_col = df2_cost_col.upper()
    new_col_name = new_col_name.upper()
    
    if df1_name in all_data and df2_name in all_data:
        df1 = all_data[df1_name]
        df2 = all_data[df2_name]

        # Standardize column names
        df1.columns = [col.upper() for col in df1.columns]
        df2.columns = [col.upper() for col in df2.columns]

        # Check if required columns exist
        missing_cols = [col for col in [df1_product_col, df1_date_col] if col not in df1.columns] + \
                       [col for col in [df2_product_col, df2_date_col, df2_cost_col] if col not in df2.columns]
        if missing_cols:
            raise KeyError(f"Missing columns in dataframes: {missing_cols}")

        # Convert dates to datetime format
        df1[df1_date_col] = pd.to_datetime(df1[df1_date_col])
        df2[df2_date_col] = pd.to_datetime(df2[df2_date_col])

        # Sort df2 (cost table) by product and date descending
        df2 = df2.sort_values(by=[df2_product_col, df2_date_col], ascending=[True, False])

        # Merge based on product and latest entry before sale date
        def get_last_cost(row):
            product = row[df1_product_col]
            sale_date = row[df1_date_col]

            # Filter cost table for matching product and valid entry dates
            valid_costs = df2[(df2[df2_product_col] == product) & (df2[df2_date_col] <= sale_date)]

            # Return the most recent cost before the sale date
            return valid_costs[df2_cost_col].iloc[0] if not valid_costs.empty else default_value

        df1[new_col_name] = df1.apply(get_last_cost, axis=1)

        # Update the dataset in all_data dictionary
        all_data[df1_name] = df1

    return all_data

def merge_data_sum(all_data, df1_name, df1_col, df2_name, df2_col, new_col, indicator_name=None, default_value=0):
    """
    Merges two datasets, summing up multiple occurrences of the same key in df2 before merging.
    
    Parameters:
    - all_data: Dictionary containing dataframes
    - df1_name: Name of the first dataframe in all_data (target dataframe)
    - df1_col: Column in df1 to match on
    - df2_name: Name of the second dataframe in all_data (source dataframe)
    - df2_col: Column in df2 to match on
    - new_col: Column in df2 that should be summed before merging
    - indicator_name: Optional, column name to indicate merge status
    - default_value: Value to use for missing matches (default = 0)
    
    Returns:
    - Updated all_data dictionary with the merged dataframe
    """
    df1_col = df1_col.upper()
    df2_col = df2_col.upper()
    new_col = new_col.upper()

    # ✅ Check if both dataframes exist
    if df1_name not in all_data:
        print(f"❌ Dataframe '{df1_name}' not found. Merge skipped.")
        return all_data
    if df2_name not in all_data:
        print(f"❌ Dataframe '{df2_name}' not found. Merge skipped.")
        return all_data

    df1 = all_data[df1_name]
    df2 = all_data[df2_name]

    # ✅ Check if source dataframe is empty (df2)
    if df2.empty:
        print(f"❌ Dataframe '{df2_name}' is empty. Merge skipped.")
        return all_data

    # Standardize column names
    df1.columns = [col.upper() for col in df1.columns]
    df2.columns = [col.upper() for col in df2.columns]

    if df1_col not in df1.columns or df2_col not in df2.columns or new_col not in df2.columns:
        raise KeyError(f"Column '{df1_col}', '{df2_col}', or '{new_col}' not found in dataframes.")

    # ✅ Aggregate df2 by summing new_col for each df2_col
    df2_agg = df2.groupby(df2_col, as_index=False)[new_col].sum()

    # ✅ Merge the summed values into df1
    merged_df = df1.merge(df2_agg, left_on=df1_col, right_on=df2_col, how='left', indicator=indicator_name, suffixes=('', '_DROP'))

    # Remove the '_DROP' columns
    merged_df.drop([col for col in merged_df.columns if col.endswith('_DROP')], axis=1, inplace=True)

    # ✅ Fill missing values with default_value
    merged_df[new_col] = merged_df[new_col].fillna(default_value)

    # ✅ If using an indicator, replace unmatched values
    if indicator_name:
        merged_df[indicator_name] = merged_df[indicator_name].apply(lambda x: default_value if x == 'left_only' else merged_df[new_col])
        merged_df.drop(columns=[new_col, indicator_name], inplace=True)

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

def compute_LPI_ANOMES(all_data):
    for key, df in all_data.items():
        # Add the ANOMES column to L_LPI
        if key == 'L_LPI' and 'DATA' in df.columns:
            df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce')  # Ensure the date is parsed correctly
            df['ANOMES'] = df['DATA'].dt.strftime('%y%m')  # Format date as YYMM
            print(f"Added ANOMES column to {key}")
        all_data[key] = df
    return all_data

def compute_ML_ANOMES(all_data):
    for key, df in all_data.items():
        # Add the ANOMES column to MLA_Vendas and MLK_Vendas
        if key in ['MLA_Vendas', 'MLK_Vendas'] and 'DATA DA VENDA' in df.columns:
            # Use dateutil parser to parse the date string
            df['DATA DA VENDA'] = df['DATA DA VENDA'].apply(lambda x: parser.parse(x, fuzzy=True) if pd.notnull(x) else pd.NaT)
            df['ANOMES'] = df['DATA DA VENDA'].dt.strftime('%y%m')  # Format date as YYMM
            print(f"Added ANOMES column to {key}")
        all_data[key] = df
    return all_data

def compute_LPI_ANOMES(all_data):
    for key, df in all_data.items():
        # Add the ANOMES column to L_LPI
        if key == 'L_LPI' and 'DATA' in df.columns:
            df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce')  # Ensure the date is parsed correctly
            df['ANOMES'] = df['DATA'].dt.strftime('%y%m')  # Format date as YYMM
            print(f"Added ANOMES column to {key}")
        all_data[key] = df
    return all_data

def compute_CC_ANOMES(all_data):
    for key, df in all_data.items():
        # Add the ANOMES column to L_LPI
        if key == 'O_CC' and 'DATA' in df.columns:
            df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce')  # Ensure the date is parsed correctly
            df['ANOMES'] = df['DATA'].dt.strftime('%y%m')  # Format date as YYMM
            print(f"Added ANOMES column to {key}")
        all_data[key] = df
    return all_data

def mlcustom_date_parser(date_str):
    # Remove the 'hs.' part if it exists
    date_str = re.sub(r'\s*hs\.\s*$', '', date_str)
    
    # Replace the Portuguese month names with English month names
    month_map = {
        'JANEIRO': 'January', 'FEVEREIRO': 'February', 'MARÇO': 'March', 'ABRIL': 'April',
        'MAIO': 'May', 'JUNHO': 'June', 'JULHO': 'July', 'AGOSTO': 'August',
        'SETEMBRO': 'September', 'OUTUBRO': 'October', 'NOVEMBRO': 'November', 'DEZEMBRO': 'December'
    }
    
    for pt_month, en_month in month_map.items():
        date_str = date_str.replace(pt_month, en_month)
    
    # Parse the date
    return pd.to_datetime(date_str, format='%d DE %B DE %Y %H:%M HS.')

def compute_ML_ANOMES(all_data):
    for key, df in all_data.items():
        # Add the ANOMES column to MLA_Vendas and MLK_Vendas
        if key in ['MLA_Vendas', 'MLK_Vendas'] and 'DATA DA VENDA' in df.columns:
            # Use custom date parser to parse the date string
            df['DATA DA VENDA'] = df['DATA DA VENDA'].apply(lambda x: mlcustom_date_parser(x) if pd.notnull(x) else pd.NaT)
            df['ANOMES'] = df['DATA DA VENDA'].dt.strftime('%y%m')  # Format date as YYMM
            print(f"Added ANOMES column to {key}")
        all_data[key] = df
    return all_data

def load_inventory_data(file_path):
    return pd.read_excel(file_path)

def print_all_tables_and_columns(all_data):
    for table_name, df in all_data.items():
        #print(f"Table: {table_name}")
        #print("Columns:", df.columns.tolist())
        print("-" * 50)

def print_table_and_columns(all_data, table_name):
    if table_name in all_data:
        #print(f"Table: {table_name}")
        #print("Columns:", all_data[table_name].columns.tolist())
        print("-" * 50)
    else:
        print(f"Table '{table_name}' not found in the dataset.")

def print_table_head(all_data, table_name):
    """
    Print the column names and the first 10 rows of a DataFrame.

    Parameters:
    df (pandas.DataFrame): The DataFrame to print.
    """
    #print("Columns:", all_data[table_name].columns.tolist())
    print("\nTop 10 Rows:")
    #print(all_data[table_name].head(10))

def excel_format(output_file, column_format_dict):
    # Load the workbook with macros
    wb = load_workbook(output_file, keep_vba=True)

    # Predefine header style if not already added
    if "header_style" not in wb.named_styles:
        header_style = NamedStyle(name="header_style")
        header_style.font = Font(bold=True)
        header_style.alignment = Alignment(horizontal="center", vertical="center")
        header_style.border = Border(
            left=Side(style="thin"),
            right=Side(style="thin"),
            top=Side(style="thin"),
            bottom=Side(style="thin")
        )
        wb.add_named_style(header_style)  # Add only if it doesn't exist

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        print(f"✅ Formatting sheet: {sheet_name}")

        # Apply column formatting first
        if sheet_name in column_format_dict:
            format_dict = column_format_dict[sheet_name]

            for col in ws.iter_cols():
                col_letter = col[0].column_letter  # Get column letter
                col_name = col[0].value  # Get header name

                if col_name in format_dict:
                    format_code = format_dict[col_name]

                    # Apply format to the entire column including header
                    for cell in col:
                        cell.number_format = format_code

        # Apply header styling separately
        for cell in ws[1]:  # First row (header)
            cell.style = "header_style"

    # **CRITICAL**: Save the workbook while keeping macros
    try:
        wb.save(output_file)
        print(f"✅ Successfully formatted and saved {output_file}")
    except Exception as e:
        print(f"❌ Error saving {output_file}: {e}")

def excel_autofilters(output_path):
    print(f"✅ Adding auto-filters to {output_path}")
    
    # Open workbook while preserving macros
    workbook = load_workbook(output_path, keep_vba=True)

    for sheetname in workbook.sheetnames:
        worksheet = workbook[sheetname]
        print(f"🔹 Processing sheet: {sheetname}")

        # Check if the worksheet has any data
        if worksheet.max_row > 1 and worksheet.max_column > 1:
            data_range = worksheet.dimensions
            if data_range and ":" in data_range:  # Ensure valid range
                worksheet.auto_filter.ref = data_range
                print(f"✅ Auto-filter applied to {sheetname}: {data_range}")
            else:
                print(f"⚠️ Skipping {sheetname}: No valid data range found.")
        else:
            print(f"⚠️ Skipping {sheetname}: Sheet is empty or has only headers.")

    # Save changes while keeping macros
    try:
        workbook.save(output_path)
        print(f"✅ Auto-filters added and saved: {output_path}")
    except Exception as e:
        print(f"❌ Error saving {output_path}: {e}")

# Define the audit function
def perform_audit(df, client_name):
    audit_columns = [
        'CODPF',
        'QTD',
        'PRECO CALC',
        'MERCVLR',
        'ICMSST',
        'IPI',
        'TOTALNF',
        'EMISS']

    audit_df = df[df['NOMEF'] == client_name][audit_columns]  
    return audit_df

def AuditMP_SH(all_data, mp2, empresa):
    lpi_columns = [
        'CÓDIGO PEDIDO',
        'EMPRESA',
        'MP',
        'MP2',
        'STATUS PEDIDO',
        'CODPP',
        'VLRVENDA',
        'QTD',
        'REPASSE'
    ]

    sh_columns = [
        'ID DO PEDIDO',
        'VALOR'
    ]
    
    # Select and filter L_LPI data
    dfa = all_data['L_LPI'][lpi_columns].copy()
    dfa = dfa[(dfa["MP2"] == mp2) & (dfa["EMPRESA"] == empresa)]

    if dfa.empty:
        print(f"Warning: No matching data found for MP2={mp2} and EMPRESA={empresa}")

    # Rename columns
    rename_map = {
        'VLRVENDA': 'VENDATOTAL',
        'REPASSE': 'REPASSEESPERADO_TODOSPEDIDOS'
    }
    dfa.rename(columns=rename_map, inplace=True)

    # Store in all_data dictionary
    all_data[f'Aud_{mp2}'] = dfa

    # Merge with SHK_Extrato
    all_data = merge_data(all_data, f'Aud_{mp2}', "CÓDIGO PEDIDO", "SHK_Extrato", "ID DO PEDIDO", "VALOR", default_value=0)

    # Ensure columns exist before renaming
    if 'VALOR' in all_data[f'Aud_{mp2}'].columns:
        all_data[f'Aud_{mp2}'].rename(columns={'VALOR': 'REPASSEEFETIVO_PEDIDOSPAGOS'}, inplace=True)
    else:
        print(f"Warning: 'VALOR' column not found after merge in Aud_{mp2}")

    # Check for missing columns before applying the lambda function
    required_columns = ['REPASSEESPERADO_TODOSPEDIDOS', 'REPASSEEFETIVO_PEDIDOSPAGOS']
    missing_columns = [col for col in required_columns if col not in all_data[f'Aud_{mp2}'].columns]

    if missing_columns:
        print(f"Error: Missing columns {missing_columns} in Aud_{mp2}")
    else:
        all_data[f'Aud_{mp2}']['REPASSEESPERADO_PEDIDOSPAGOS'] = all_data[f'Aud_{mp2}'].apply(
            lambda row: 0 if row['REPASSEEFETIVO_PEDIDOSPAGOS'] == 0 else row['REPASSEESPERADO_TODOSPEDIDOS'], axis=1
        )

    return all_data

def AuditMP_ML(all_data, mp2, empresa):
    lpi_columns = [
        'CÓDIGO PEDIDO',
        'EMPRESA',
        'MP',
        'MP2',
        'STATUS PEDIDO',
        'CODPP',
        'VLRVENDA',
        'QTD',
        'REPASSE'
    ]

    sh_columns = [
        'ORDER_ID',
        'NETVALUE',
        'DESC'
    ]
    
    # Select and filter L_LPI data
    dfa = all_data['L_LPI'][lpi_columns].copy()
    dfa = dfa[(dfa["MP2"] == mp2) & (dfa["EMPRESA"] == empresa)]

    if dfa.empty:
        print(f"Warning: No matching data found for MP2={mp2} and EMPRESA={empresa}")

    # Rename columns
    rename_map = {
        'VLRVENDA': 'VENDATOTAL',
        'REPASSE': 'REPASSEESPERADO_TODOSPEDIDOS'
    }
    dfa.rename(columns=rename_map, inplace=True)

    # Store in all_data dictionary
    all_data[f'Aud_{mp2}'] = dfa

    # Merge with SHK_Extrato
    all_data = merge_data_sum(all_data, f'Aud_{mp2}', "CÓDIGO PEDIDO", "MLK_ExtLib", "ORDER_ID", "NETVALUE", default_value=0)

    # Ensure columns exist before renaming
    if 'NETVALUE' in all_data[f'Aud_{mp2}'].columns:
        all_data[f'Aud_{mp2}'].rename(columns={'NETVALUE': 'REPASSEEFETIVO_PEDIDOSPAGOS'}, inplace=True)
    else:
        print(f"Warning: 'VALOR' column not found after merge in Aud_{mp2}")

    # Check for missing columns before applying the lambda function
    required_columns = ['REPASSEESPERADO_TODOSPEDIDOS', 'REPASSEEFETIVO_PEDIDOSPAGOS']
    missing_columns = [col for col in required_columns if col not in all_data[f'Aud_{mp2}'].columns]

    if missing_columns:
        print(f"Error: Missing columns {missing_columns} in Aud_{mp2}")
    else:
        all_data[f'Aud_{mp2}']['REPASSEESPERADO_PEDIDOSPAGOS'] = all_data[f'Aud_{mp2}'].apply(
            lambda row: 0 if row['REPASSEEFETIVO_PEDIDOSPAGOS'] == 0 else row['REPASSEESPERADO_TODOSPEDIDOS'], axis=1
        )

    return all_data

def AuditMP_MA(all_data, mp2, empresa):
    lpi_columns = [
        'CÓDIGO PEDIDO',
        'EMPRESA',
        'MP',
        'MP2',
        'STATUS PEDIDO',
        'CODPP',
        'VLRVENDA',
        'QTD',
        'REPASSE'
    ]

    sh_columns = [
        'NÚMERO DO PEDIDO',
        'VALOR LÍQUIDO ESTIMADO A RECEBER (****)',
        'DESC'
    ]
    
    # Select and filter L_LPI data
    dfa = all_data['L_LPI'][lpi_columns].copy()
    dfa = dfa[(dfa["MP2"] == mp2) & (dfa["EMPRESA"] == empresa)]

    if dfa.empty:
        print(f"Warning: No matching data found for MP2={mp2} and EMPRESA={empresa}")

    # Rename columns
    rename_map = {
        'VLRVENDA': 'VENDATOTAL',
        'REPASSE': 'REPASSEESPERADO_TODOSPEDIDOS'
    }
    dfa.rename(columns=rename_map, inplace=True)

    # Store in all_data dictionary
    all_data[f'Aud_{mp2}'] = dfa

    # Merge with SHK_Extrato
    all_data = merge_data_sum(all_data, f'Aud_{mp2}', "CÓDIGO PEDIDO", "MGK_Extrato", "NÚMERO DO PEDIDO", "VALOR LÍQUIDO ESTIMADO A RECEBER (****)", default_value=0)

    # Ensure columns exist before renaming
    if 'VALOR LÍQUIDO ESTIMADO A RECEBER (****)' in all_data[f'Aud_{mp2}'].columns:
        all_data[f'Aud_{mp2}'].rename(columns={'VALOR LÍQUIDO ESTIMADO A RECEBER (****)': 'REPASSEEFETIVO_PEDIDOSPAGOS'}, inplace=True)
    else:
        print(f"Warning: 'VALOR' column not found after merge in Aud_{mp2}")

    # Check for missing columns before applying the lambda function
    required_columns = ['REPASSEESPERADO_TODOSPEDIDOS', 'REPASSEEFETIVO_PEDIDOSPAGOS']
    missing_columns = [col for col in required_columns if col not in all_data[f'Aud_{mp2}'].columns]

    if missing_columns:
        print(f"Error: Missing columns {missing_columns} in Aud_{mp2}")
    else:
        all_data[f'Aud_{mp2}']['REPASSEESPERADO_PEDIDOSPAGOS'] = all_data[f'Aud_{mp2}'].apply(
            lambda row: 0 if row['REPASSEEFETIVO_PEDIDOSPAGOS'] == 0 else row['REPASSEESPERADO_TODOSPEDIDOS'], axis=1
        )

    return all_data


# Define the function to perform audits for all specified clients
def perform_all_MP_audits(all_data):
    all_data = AuditMP_SH(all_data, 'SH','K')
    all_data = AuditMP_ML(all_data, 'ML','K')
    all_data = AuditMP_MA(all_data, 'MA','K')
    return all_data


# Define the function to perform audits for all specified clients
def perform_all_audits(all_data):
    for client_name in audit_client_names:
        audit_df = perform_audit(all_data['O_NFCI'], client_name)
        all_data[f'Audit_{client_name}'] = audit_df
        print(f"Performed audit for {client_name}")  # Debug print
    return all_data

# Function to track inventory movements
# Function to calculate realized cost
def track_inventory(sales_data, purchase_data):
    inventory_movements = []

    # Process sales data
    for index, row in sales_data.iterrows():
        movement = {
            'Date': row['DATA'],
            'Invoice Number': None,
            'Product Code': row['CODPF'],
            'Quantity': -row['QTD'],
            'CV': 'V',
            'QTD E': row['QTD'],
            'CMV Unit E': None,
            'CMV Mov E': None,
            'QTD R': None,
            'CMV Unit R': None,
            'CMV Mov R': None,
            'NF Compra': None
        }
        inventory_movements.append(movement)

    # Process purchase data
    for index, row in purchase_data.iterrows():
        movement = {
            'Date': row['EMISS'],
            'Invoice Number': row['NF'],
            'Product Code': row['CODPF'],
            'Quantity': row['QTD'],
            'CV': 'C',
            'QTD E': None,
            'CMV Unit E': row['PRECO CALC'],
            'CMV Mov E': row['MERCVLR'],
            'QTD R': None,
            'CMV Unit R': None,
            'CMV Mov R': None,
            'NF Compra': row['NF'],
            'Custo Total Unit': row['TOTALNF'] / row['QTD']
        }
        inventory_movements.append(movement)

    inventory_df = pd.DataFrame(inventory_movements)
    inventory_df.sort_values(by='Date', inplace=True)
    return inventory_df

def calculate_realized_cost(inventory_df):
    # Get all purchases (C) in ascending date order
    purchase_data = inventory_df[inventory_df['CV'] == 'C'].sort_values(by='Date')

    # Create a list of purchases as objects with necessary details
    purchase_list = []
    for _, row in purchase_data.iterrows():
        purchase_list.append({
            'Product Code': row['Product Code'],
            'Invoice Number': row['Invoice Number'],
            'Quantity': row['Quantity'],
            'Custo Total Unit': row['Custo Total Unit']
        })
    #print("Purchase List:", purchase_list)  # Debug print

    # Iterate through the sales (V) and populate the realized cost details
    for index, row in inventory_df[inventory_df['CV'] == 'V'].iterrows():
        quantity_needed = -row['Quantity']
        for purchase in purchase_list:
            if purchase['Product Code'] == row['Product Code'] and quantity_needed > 0:
                if purchase['Quantity'] > 0:
                    quantity_to_apply = min(purchase['Quantity'], quantity_needed)

                    # Update the realized cost details
                    inventory_df.at[index, 'QTD R'] = quantity_to_apply
                    inventory_df.at[index, 'CMV Unit R'] = purchase['Custo Total Unit']
                    inventory_df.at[index, 'CMV Mov R'] = quantity_to_apply * purchase['Custo Total Unit']
                    inventory_df.at[index, 'NF Compra'] = purchase['Invoice Number']

                    # Update the purchase details
                    purchase['Quantity'] -= quantity_to_apply
                    quantity_needed -= quantity_to_apply

        # If there's still quantity needed, populate the expected cost details
        if quantity_needed > 0:
            inventory_df.at[index, 'QTD E'] = quantity_needed
            if row['CMV Unit E'] is not None:
                inventory_df.at[index, 'CMV Mov E'] = quantity_needed * row['CMV Unit E']

    # Add remaining purchase quantities back to the corresponding purchase rows
    for purchase in purchase_list:
        if purchase['Quantity'] > 0:
            purchase_indices = inventory_df[
                (inventory_df['Product Code'] == purchase['Product Code']) &
                (inventory_df['Invoice Number'] == purchase['Invoice Number'])
            ].index
            if not purchase_indices.empty:
                purchase_index = purchase_indices[0]
                inventory_df.at[purchase_index, 'QTD E'] = purchase['Quantity']
                inventory_df.at[purchase_index, 'CMV Unit E'] = purchase['Custo Total Unit']
                inventory_df.at[purchase_index, 'CMV Mov E'] = purchase['Quantity'] * purchase['Custo Total Unit']

    #print(inventory_df)  # Debug print
    return inventory_df

# Function to perform the inventory audit
def perform_invaudit(o_nfci_df, l_lpi_df, client_name):
    sales_data = l_lpi_df[l_lpi_df['EMPRESAF'] == client_name]
    purchase_data = o_nfci_df[o_nfci_df['NOMEF'] == client_name]

    inventory_df = track_inventory(sales_data, purchase_data)
    inventory_df = calculate_realized_cost(inventory_df)
    return inventory_df

# Function to perform all inventory audits
def perform_all_invaudits(all_data):
    o_nfci_df = all_data['O_NFCI']
    l_lpi_df = all_data['L_LPI']

    invaudit_results = {}
    for client in invaudit_client_names:
        invaudit_results[client] = perform_invaudit(o_nfci_df, l_lpi_df, client)

    # Add the audit results to the all_data dictionary
    for client, df in invaudit_results.items():
        all_data[f'InvAudit_{client}'] = df

    return all_data

def main():
    # Define file patterns for each data type
    file_patterns = {
        'O_NFCI': 'O_NFCI_{year_month}_clean.xlsx',
        'L_LPI': 'L_LPI_{year_month}_clean.xlsx',
        'MLA_Vendas': 'MLA_Vendas_{year_month}_clean.xlsx',
        'MLK_Vendas': 'MLK_Vendas_{year_month}_clean.xlsx',
        'O_CC': 'O_CC_{year_month}_clean.xlsx',
        'O_CtasAPagar': 'O_CtasAPagar_{year_month}_clean.xlsx',
        'O_CtasARec': 'O_CtasARec_{year_month}_clean.xlsx',
        'MLK_ExtLib': 'MLK_ExtLib_{year_month}_clean.xlsx',
        'SHK_Extrato': 'SHK_Extrato_{year_month}_clean.xlsx',
        'MGK_Pacotes': 'MGK_Pacotes_{year_month}_clean.xlsx',
        'MGK_Extrato': 'MGK_Extrato_{year_month}_clean.xlsx',
    }

    all_data = {}

    for key, pattern in file_patterns.items():
        recent_data = load_recent_data(base_dir, pattern)
        print(f"{key} data shape: {recent_data.shape}")  # Debug print

        # Ensure 'N.º de venda' is treated as string if the column exists
        #if 'N.º de venda' in recent_data.columns:
        #    recent_data['N.º de venda'] = recent_data['N.º de venda'].astype(str)
        #if 'N.º de venda_hyperlink' in recent_data.columns:
        #    recent_data['N.º de venda_hyperlink'] = recent_data['N.º de venda_hyperlink'].astype(str)

        # Ensure long numeric IDs remain as text before storing them
        string_columns = ["ORDER_ID", "TRANSACTION_ID", "N.º de venda", "N.º de venda_hyperlink", "SHIPPING_ID", "SOURCE_ID", "EXTERNAL_REFERENCE"]  # Add more if needed

        for col in string_columns:
            if col in recent_data.columns:
                recent_data[col] = recent_data[col].astype(str)  # Ensure stored as text
                print(f"Changed {col} to str. Sample values: {recent_data[col].head().tolist()}")

        all_data[key] = recent_data

    # Load static data
    static_tables = ['T_CondPagto.xlsx', 'T_Fretes.xlsx', 'T_GruposCli.xlsx', 'T_MP.xlsx', 
                     'T_RegrasMP.xlsx', 'T_Remessas.xlsx', 'T_Reps.xlsx', 'T_Verbas.xlsx', 'T_Vol.xlsx', 'T_ProdF.xlsx', 
                     'T_ProdP.xlsx', 'T_Entradas.xlsx', 'T_FretesMP.xlsx', 'T_MLStatus.xlsx', 'T_CtasAPagarClass.xlsx',
                     'T_CtasARecClass.xlsx', 'T_CCCats.xlsx']
    static_data_dict = {table.replace('.xlsx', ''): load_static_data(static_dir, table) for table in static_tables}
    
    # Check static data shapes
    for key, df in static_data_dict.items():
        print(f"Static data {key} shape: {df.shape}")  # Debug print
    
    #inventory_data = preprocess_inventory_data(inventory_file_path)

    # Add static data to all_data dictionary
    all_data.update(static_data_dict)
    #all_data.update(inventory_data)
    
    all_data = rename_columns(all_data, column_rename_dict)

    # Merge all data with static data
    all_data = merge_all_data(all_data) 

    # Perform audits for the specified Marketplaces
    all_data = perform_all_MP_audits(all_data)
    print(f"Audit completed for ALL MARKETPLACES")

    # Perform audits for the specified clients
    all_data = perform_all_audits(all_data)
    print(f"Audit completed for clients: {', '.join(audit_client_names)}")

    # Perform inventory audits for the specified clients
    all_data = perform_all_invaudits(all_data)
    print(f"INVENTORY Audit completed for clients: {', '.join(audit_client_names)}")

   # Write to the existing template (wb_template)
    for key, df in all_data.items():
        ws = wb_template.create_sheet(title=key)
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)
        print(f"✅ Added {key} data to {output_file} in sheet {key}")  # Debug print

    # Save the modified workbook
    wb_template.save(output_file)
    print(f"✅ All merged data saved to {output_file}")

    excel_format(output_file, column_format_dict)
    excel_autofilters(output_file)

if __name__ == "__main__":
    main()
