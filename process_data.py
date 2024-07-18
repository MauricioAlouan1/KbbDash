import re
import os
import openpyxl
import pandas as pd
import numpy as np

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


def find_header_row(filepath, header_name):
    """Find the header row index that contains a specific header name in an Excel file, handle merged cells."""
    temp_data = pd.read_excel(filepath, header=None)
    for index, row in temp_data.iterrows():
        if header_name in row.values:
            print(f"Header '{header_name}' found at row index: {index}")
            return index
    raise ValueError(f"Header {header_name} not found in the file.")

def process_O_NFSI(data):
    if 'Operação' in data.columns:
        data['Operação'] = data['Operação'].ffill()  # Forward fill to handle empty cells
    # Remove row where the specific column has 'Total geral'
    data = data[data['Operação'] != 'Total geral']  # Adjust 'Operação' to your actual column name
    return data

def process_O_NFCI(data):
    """Inspect and process O_NFCI files: Remove rows where 'Situação' is effectively blank."""
    if not data.empty:
        # Print unique values in 'Situação' to inspect what's being considered as blank
        ### print("Unique values in 'Situação':", data['Situação'].dropna().unique())
        # Remove rows where 'Situação' appears to be blank or any unexpected content
        data = data[data['Situação'].apply(lambda x: x not in [None, '', ' ', np.nan, np.float64])]
    return data

def process_O_CC(data):
    """Process O_CC files: additional specific requirements here, if any."""
    # Assuming 'Valor (R$)' is in column F and needs to be non-zero
    data['Valor (R$)'] = pd.to_numeric(data['Valor (R$)'], errors='coerce')  # Ensure numeric
    data = data[data['Valor (R$)'] != 0]  # Remove rows where 'Valor (R$)' is zero
    return data

def process_O_CtasAPagar(data):
    """Process O_CtasAPagar files: remove the row immediately below the headers."""
    # Remove the first row below the headers
    if not data.empty:
        data = data.iloc[1:]  # Remove the first row, which could be totals or sub-headers
    return data

def process_O_Estoq(data):
    """Process O_Estoq files: adapt this function to meet specific requirements."""
    # Example: Remove rows where 'Código do Produto' is empty
    data = data[data['Código do Produto'].notna()]
    return data

def process_B_Estoq(data):
    """Process B_Estoq files: convert number formats in 'Quantidade', remove rows with 'Quantidade' = 0, and remove the last row."""
    if not data.empty:
        # Convert 'Quantidade' column to correct numeric format, considering "." as thousands separator and "," as decimal
        data['Quantidade'] = data['Quantidade'].replace(r'\.', '', regex=True).replace(',', '.', regex=True).astype(float)        
        # Remove rows where 'Quantidade' is 0
        data = data[data['Quantidade'] != 0]        
        # Remove the last row of the DataFrame
        data = data.iloc[:-1]
    return data

def process_L_LPI(data):
    """Process L_LPI files: convert formatted currency in specific columns to float."""
    currency_columns = ['Preço', 'Preço Total', 'Preço Com Desconto', 'Desconto Total',
                        'Desconto Pedido', 'Desconto Item', 'Desconto Total',
                        'Desconto Pedido Seller', 'Desconto Item Seller', 'Comissão',
                        'Frete Seller', 'Frete Comprador', 'Acrescimo', 'Recebimento', 'Custo',
                        'Custo Total', 'Imposto', 'Lucro Bruto']  # Update if more columns are involved
    for col in currency_columns:
        if col in data.columns:
            data[col] = data[col].apply(convert_currency_to_float)
    return data

def process_MLK_Vendas(data):
    """Process MLK_Vendas files."""
    # Example processing: remove rows where 'N.º de venda' is NaN
    data = process_ml_data(data)
    #data = data[data['N.º de venda'].notna()]
    return data

def process_ml_data(df):
    # Ensure the required columns exist before processing
    required_columns = ['N.º de venda', 'SKU', 'Receita por produtos (BRL)', 'Receita por envio (BRL)', 'Tarifa de venda e impostos', 'Tarifas de envio', 'Cancelamentos e reembolsos (BRL)']
    if not all(col in df.columns for col in required_columns):
        raise ValueError("Dataframe does not contain all required columns.")

    # Strip any whitespace from column names
    df.columns = df.columns.str.strip()

    # Rename only the first occurrence of 'Unidades'
    unidades_columns = [i for i, col in enumerate(df.columns) if col == 'Unidades']
    if unidades_columns:
        first_unidades_index = unidades_columns[0]
        df.columns.values[first_unidades_index] = 'Quantidade'

    # Convert 'Unidades' to numeric, coerce errors to NaN, and then fill NaN with 0
    df['Quantidade'] = pd.to_numeric(df['Quantidade'], errors='coerce').fillna(0)

    # Step 1: Calculate the number of unique SKUs per order (excluding NaN SKUs)
    # Adjust the SKUs in Order count if it's greater than 1
    df['SKUs in Order'] = df[df['SKU'].notna()].groupby('N.º de venda_hyperlink')['SKU'].transform('nunique')
    df['SKUs in Order'] = df['SKUs in Order'].apply(lambda x: x-1 if x > 1 else x)


    # Step 2: Calculate the total number of items per order
    df['Items in Order'] = df.groupby('N.º de venda_hyperlink')['Quantidade'].transform('sum')


    # Calculate the proportional values
    #df['Proportional Valor da Venda'] = df['Valor da Venda'] / df['Total Items']
    #df['Proportional Tarifa ML'] = df['Tarifa ML'] / df['Total Items']
    #df['Proportional Frete'] = df['Frete'] / df['Total Items']
    #df['Proportional Custo de Envio'] = df['Custo de Envio'] / df['Total Items']
    #df['Proportional Custo'] = df['Custo'] / df['Total Items']
    #df['Proportional Lucro'] = df['Lucro'] / df['Total Items']
    
    # Keep only the SKU rows
    #df = df.drop_duplicates(subset=['SKU'], keep='first')
    
    # Drop the package rows
    #df = df.drop(columns=['Total Items', 'Valor da Venda', 'Tarifa ML', 'Frete', 'Custo de Envio', 'Custo', 'Lucro'])
    
    return df

def excel_column_range(start, end):
    """Generate Excel column labels between start and end inclusive."""
    columns = []
    start_index = int(start, 36) - 9  # Convert letter to number (base 36 to decimal, adjusted for Excel)
    end_index = int(end, 36) - 9
    for i in range(start_index, end_index + 1):
        number = i
        col = ''
        while number > 0:
            number, remainder = divmod(number - 1, 26)
            col = chr(65 + remainder) + col
        columns.append(col)
    return columns

def load_and_clean_data(filepath, processor, header_name, extract_hyperlinks=False):
    """Load data from an Excel file, handle merged headers, optionally extract hyperlinks."""
    if extract_hyperlinks:  
        # Call a separate function dedicated to extracting hyperlinks
        data = extract_hyperlinks_data(filepath, header_name)
    else:
        # Continue with the original data loading method
        header_row_index = find_header_row(filepath, header_name)
        data = pd.read_excel(filepath, header=header_row_index)    
    # Extract month and year from the filename and add as a new column if necessary
    if processor in [process_B_Estoq, process_O_CtasAPagar, process_O_Estoq]:
        month_year = int(extract_month_year_from_filename(filepath))
        data['AnoMes'] = month_year
    # Process the data using the specified processor function
    return processor(data)

def extract_month_year_from_filename(filename):
    """Extract month and year from the filename in the format YYMM."""
    base_name = os.path.basename(filename)
    match = re.search(r'(\d{4})_(\d{2})', base_name)
    if match:
        year = match.group(1)[-2:]  # Get the last two digits of the year
        month = match.group(2)  # Get the month
        return f"{year}{month}"
    else:
        return "Unknown"

def convert_currency_to_float(currency_str):
    """Convert currency string 'R$ 149,90' to float 149.90, handle mixed data types."""
    if pd.isna(currency_str):
        return None  # Handle missing values
    # Check if the value is already a numeric type (float or int)
    if isinstance(currency_str, (int, float)):
        return float(currency_str)  # Return as float if already a number
    # Assuming the value is a string that needs to be cleaned and converted
    try:
        # Remove 'R$', replace ',' with '.', and remove any spaces or periods used as thousands separators
        normalized_str = currency_str.replace('R$', '').replace(' ', '').replace('.', '').replace(',', '.').strip()
        return float(normalized_str)
    except ValueError:
        print(f"Conversion error with input '{currency_str}'")
        return None
    
def check_and_process_files():
    raw_dir = os.path.join(base_dir, 'raw')
    clean_dir = os.path.join(base_dir, 'clean')

    processing_map = {
        'O_NFSI': (process_O_NFSI, "Operação", False),
        'O_NFCI': (process_O_NFCI, "Operação", False),
        'O_CC': (process_O_CC, "Situação", False),
        'O_CtasAPagar': (process_O_CtasAPagar, "Minha Empresa (Nome Fantasia)", False),
        'O_CtasARec': (process_O_CtasAPagar, "Minha Empresa (Nome Fantasia)", False),
        'B_Estoq': (process_B_Estoq, "Código", False),
        'L_LPI': (process_L_LPI, "Data", False),
        'O_Estoq': (process_O_Estoq, "Código do Produto", False),
        'MLK_Vendas': (process_MLK_Vendas, "N.º de venda", True),  # Enable hyperlink extraction for MLK_Vendas
        'MLA_Vendas': (process_MLK_Vendas, "N.º de venda", True)  # New entry, same process as MLK_Vendas
    }
    for subdir, dirs, files in os.walk(raw_dir):
        for file in files:
            if file.endswith('.xlsx') and not file.startswith('~$'):
                # Loop through each file type in the processing map
                for key, (processor, header_name, use_hyperlinks) in processing_map.items():
                    if key in file:  # Check if the file type matches the key in the map
                        raw_filepath = os.path.join(subdir, file)
                        clean_subdir = os.path.join(clean_dir, os.path.basename(subdir))
                        clean_filepath = os.path.join(clean_subdir, file.replace('.xlsx', '_clean.xlsx'))
                        
                        if not os.path.exists(clean_filepath):
                            print(f"Processing {file}...")
                            try:
                                data = load_and_clean_data(raw_filepath, processor, header_name, use_hyperlinks)
                                save_cleaned_data(data, clean_filepath)
                            except Exception as e:
                                print(f"Error processing {file}: {e}")
                        else:
                            print(f"Skipped {file}, already processed.")

def extract_hyperlinks_data(filepath, header_name):
    """Extract data and create a new column for hyperlinks for a specific header."""
    wb = openpyxl.load_workbook(filepath, data_only=False)
    ws = wb.active
    data_rows = []
    header_row_index = None
    headers = []

    # Iterate over rows to find the header and extract data
    for row in ws.iter_rows(min_row=1, max_col=ws.max_column, values_only=False):
        if header_row_index is None:
            if any(header_name == (cell.value or '') for cell in row):
                header_row_index = row[0].row
                headers = [cell.value for cell in row]
                headers.append(f"{header_name}_hyperlink")
                continue
        if header_row_index and row[0].row > header_row_index:
            row_data = []
            hyperlink_value = None
            for cell in row:
                if cell.column == headers.index(header_name) + 1 and cell.hyperlink:
                    # Replace specific parts of the hyperlink
                    hyperlink_replaced = cell.hyperlink.target.replace("https://www.mercadolivre.com.br/vendas/", "").replace("/detalhe#source=excel", "")
                    hyperlink_value = hyperlink_replaced
                row_data.append(cell.value)
            row_data.append(hyperlink_value)
            data_rows.append(row_data)

    return pd.DataFrame(data_rows, columns=headers)

def find_header_row(filepath, header_name):
    """Utility function to find the header row index using pandas."""
    for i, row in pd.read_excel(filepath, header=None).iterrows():
        if header_name in row.values:
            return i
    raise ValueError(f"Header {header_name} not found in the file.")

def save_cleaned_data(data, output_filepath):
    """Save the cleaned data to a new Excel file."""
    os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
    data.to_excel(output_filepath, index=False)

if __name__ == "__main__":
    check_and_process_files()
