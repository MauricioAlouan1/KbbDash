import os
import pandas as pd

# Define the base directory as before, now adding the /clean part
path_options = [
    '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/',
    '/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data'
]
for path in path_options:
    if os.path.exists(path):
        base_dir = path
        break
else:
    print("None of the specified directories exist.")
    base_dir = None

# Define the date range variables
start_year = 2024
start_month = 9
end_year = 2024
end_month = 9

# Function to process inventory files for a given month
def process_inventory_files(month):
    """Process and stack inventory files for a given month."""
    try:
        # The files for each month are inside the /clean/2024_MM/ folder
        clean_folder = os.path.join(base_dir, f'clean/2024_{month}')

        b_estoq_file = f'B_Estoq_2024_{month}_clean.xlsx'
        t_esttrans_file = f'T_EstTrans_2024_{month}_clean.xlsx'
        o_estoq_file = f'O_Estoq_2024_{month}_clean.xlsx'

        # Load the files from the correct monthly folder
        b_estoq = pd.read_excel(os.path.join(clean_folder, b_estoq_file), usecols=['Código', 'Quantidade'])
        t_esttrans = pd.read_excel(os.path.join(clean_folder, t_esttrans_file), usecols=['CodProd', 'Qt'])
        o_estoq = pd.read_excel(os.path.join(clean_folder, o_estoq_file), usecols=['Código do Produto', 'Quantidade', 'Local de Estoque (Código)'])

        # Rename columns for consistency
        b_estoq.columns = ['Codigo', 'Quantidade']
        t_esttrans.columns = ['Codigo', 'Quantidade']
        o_estoq.columns = ['Codigo', 'Quantidade', 'Local']

        # Add the 'Local' column to b_estoq with a default value of 'Bling'
        b_estoq['Local'] = 'Bling'

        # Add the 'Local' column to t_esttrans with a default value of 'Transito'
        t_esttrans['Local'] = 'Transito'

        # Reset index to avoid any alignment issues and ensure columns are aligned
        b_estoq.reset_index(drop=True, inplace=True)
        t_esttrans.reset_index(drop=True, inplace=True)
        o_estoq.reset_index(drop=True, inplace=True)

        # Stack all inventory data into one dataframe
        combined_df = pd.concat([b_estoq, t_esttrans, o_estoq], ignore_index=True)
        
        return combined_df

    except Exception as e:
        print(f"Error processing inventory files for {month}: {e}")
        return None

# Function to lookup CU values and additional columns
def lookup_cu_values(inventory_df):
    """Lookup various CU values and perform additional calculations."""
    try:
        # Load T_Entradas.xlsx and T_ProdF.xlsx
        entradas_df = pd.read_excel(os.path.join(base_dir, 'Tables', 'T_Entradas.xlsx'))
        prodf_df = pd.read_excel(os.path.join(base_dir, 'Tables', 'T_ProdF.xlsx'))

        # Ensure unique values in key columns to avoid duplicates
        entradas_df = entradas_df.drop_duplicates(subset=['Pai', 'Filho'])
        prodf_df = prodf_df.drop_duplicates(subset=['CodPF'])

        # Filter T_Entradas where X = 1
        filtered_entradas = entradas_df[entradas_df['X'] == 1]

        # Match 'Codigo' to T_ProdF['CodPF'] to get T_ProdF['CodPP'] as 'CodPP'
        inventory_df = pd.merge(inventory_df, prodf_df[['CodPF', 'CodPP']], left_on='Codigo', right_on='CodPF', how='left')

        # Create UCP by matching CodPP to T_Entradas[Pai]
        inventory_df = pd.merge(inventory_df, filtered_entradas[['Pai', 'Ult CU R$']].rename(columns={'Ult CU R$': 'UCP'}), 
                                left_on='CodPP', right_on='Pai', how='left')

        # Create UCF by matching Codigo to T_Entradas[Filho]
        inventory_df = pd.merge(inventory_df, filtered_entradas[['Filho', 'Ult CU R$']].rename(columns={'Ult CU R$': 'UCF'}), 
                                left_on='Codigo', right_on='Filho', how='left')

        # Ensure 'Quantidade', 'UCP', and 'UCF' are numeric (force conversion)
        inventory_df['Quantidade'] = pd.to_numeric(inventory_df['Quantidade'], errors='coerce').fillna(0)
        inventory_df['UCP'] = pd.to_numeric(inventory_df['UCP'], errors='coerce').fillna(0)
        inventory_df['UCF'] = pd.to_numeric(inventory_df['UCF'], errors='coerce').fillna(0)

        # Create UCU: If UCP > 0, use UCP; otherwise, use UCF
        inventory_df['UCU'] = inventory_df.apply(lambda row: row['UCP'] if row['UCP'] > 0 else row['UCF'], axis=1)

        # Create UCT: UCU * Quantidade
        inventory_df['UCT'] = inventory_df['UCU'] * inventory_df['Quantidade']

        return inventory_df

    except Exception as e:
        print(f"Error looking up CU values: {e}")
        return None

# Main function to handle the process for all months within the date range
def process_all_months():
    # Loop through each year and month in the specified range
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == start_year and month < start_month:
                continue
            if year == end_year and month > end_month:
                break

            month_str = f"{month:02d}"  # Format the month as 01, 02, ..., 12
            print(f"Processing data for month: {month_str}")

            # Step 1: Process and stack inventory data for the month
            inventory_df = process_inventory_files(month_str)
            if inventory_df is None:
                continue

            # Step 2: Lookup CU values and calculate UCU and UCT
            final_df = lookup_cu_values(inventory_df)
            if final_df is None:
                continue

            # Step 3: Save the resulting dataframe to a new Excel file
            output_filepath = os.path.join(base_dir, 'clean', f'combined_inventory_{month_str}.xlsx')
            final_df.to_excel(output_filepath, index=False)
            print(f"Saved combined inventory data for {month_str} to {output_filepath}")

if __name__ == "__main__":
    process_all_months()
