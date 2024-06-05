import os
import pandas as pd
import numpy as np

# Base directory where the raw and clean data are stored
base_dir = '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/'


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
    """Process B_Estoq files: convert number formats in 'Quantidade' and remove the last row."""
    if not data.empty:
        # Convert 'Quantidade' column to correct numeric format, considering "." as thousands separator and "," as decimal
        data['Quantidade'] = data['Quantidade'].replace(r'\.', '', regex=True).replace(',', '.', regex=True).astype(float)
        
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

def load_and_clean_data(filepath, processor, header_name):
    """Load data from an Excel file, manually handle merged headers."""
    header_row_index = find_header_row(filepath, header_name)
    print(f"Using header row index: {header_row_index}")
    data = pd.read_excel(filepath, header=None)
    headers = data.iloc[header_row_index].tolist()  # Assuming the headers are in the specified row
    data.columns = headers
    data = data.iloc[header_row_index + 1:]  # Skip the header row and anything above it
    print("Loaded column headers:", data.columns)  # Print actual column names
    return processor(data)

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
        'O_NFSI': (process_O_NFSI, "Operação"),  # Use process_O_NFSI for O_NFSI files
        'O_NFCI': (process_O_NFCI, "Operação"),  # Use process_O_NFCI for O_NFCI files
        'O_CC': (process_O_CC, "Situação"),  # Assuming 'Situação' is the header of interest
        'O_CtasAPagar': (process_O_CtasAPagar, "Minha Empresa (Nome Fantasia)"),  # Add new report processing
        'O_CtasARec': (process_O_CtasAPagar, "Minha Empresa (Nome Fantasia)"),  # Same structure as O_CtasAPagar
        'B_Estoq': (process_B_Estoq, "Código"),  # New entry for B_Estoq
        'L_LPI': (process_L_LPI, "Data"),  # Add new entry for L_LPI
        'O_Estoq': (process_O_Estoq, "Código do Produto")  # New entry for O_Estoq
    }

    for subdir, dirs, files in os.walk(raw_dir):
        for file in files:
            if file.endswith('.xlsx') and not file.startswith('~$'):
                # Loop through each file type in the processing map
                for key, (processor, header_name) in processing_map.items():
                    if key in file:  # Check if the file type matches the key in the map
                        raw_filepath = os.path.join(subdir, file)
                        clean_subdir = os.path.join(clean_dir, os.path.basename(subdir))
                        clean_filepath = os.path.join(clean_subdir, file.replace('.xlsx', '_clean.xlsx'))
                        
                        if not os.path.exists(clean_filepath):
                            print(f"Processing {file}...")
                            try:
                                data = load_and_clean_data(raw_filepath, processor, header_name)
                                save_cleaned_data(data, clean_filepath)
                            except Exception as e:
                                print(f"Error processing {file}: {e}")
                        else:
                            print(f"Skipped {file}, already processed.")



def save_cleaned_data(data, output_filepath):
    """Save the cleaned data to a new Excel file."""
    os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
    data.to_excel(output_filepath, index=False)


if __name__ == "__main__":
    check_and_process_files()

