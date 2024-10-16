import os
import pandas as pd
import sys

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

# Function to process inventory files for a given month and year
def process_inventory_files(year, month):
    """Process and stack inventory files for a given year and month."""
    try:
        # Format the month to always be two digits (e.g., 01, 02, ..., 12)
        month_str = f'{month:02d}'
        
        # The files for each month are inside the /clean/YYYY_MM/ folder
        clean_folder = os.path.join(base_dir, f'clean/{year}_{month_str}')

        b_estoq_file = f'B_Estoq_{year}_{month_str}_clean.xlsx'
        t_esttrans_file = f'T_EstTrans_{year}_{month_str}_clean.xlsx'
        o_estoq_file = f'O_Estoq_{year}_{month_str}_clean.xlsx'

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
        print(f"Error processing inventory files for {year}-{month_str}: {e}")
        return None

# Function to lookup CU values and additional columns
# Function to lookup CU values and additional columns
def lookup_cu_values(inventory_df):
    print("inventory_df")
    print(inventory_df)
    print(f"inventory_df shape: {inventory_df.shape}")

    """Lookup various CU values and perform additional calculations."""
    try:
        # Load T_Entradas.xlsx, ensuring Pai and Filho are treated as text
        entradas_df = pd.read_excel(
            os.path.join(base_dir, 'Tables', 'T_Entradas.xlsx'),
            dtype={'Pai': str, 'Filho': str}  # Treat Pai and Filho as text
        )
        # Print head of df
        print("entradas_df")
        print(entradas_df.head())
        print(f"entradas_df shape: {entradas_df.shape}")

        # Load T_ProdF.xlsx, ensuring CodPF and CodPP are treated as text
        prodf_df = pd.read_excel(
            os.path.join(base_dir, 'Tables', 'T_ProdF.xlsx'),
            dtype={'CodPF': str, 'CodPP': str}  # Treat CodPF and CodPP as text
        )
        # Print head of df
        print("prodf_df")
        print(prodf_df.head())
        print(f"prodf_df shape: {prodf_df.shape}")

        # Ensure unique values in key columns to avoid duplicates
        # Filter T_Entradas where X = 1
        filtered_entradas = entradas_df[entradas_df['X'] == 1]
        print("filtered_entradas")
        print(filtered_entradas.head())
        print(f"filtered_entradas shape: {filtered_entradas.shape}")

        # Ensure that 'Codigo_Inv' is treated as text in inventory_df
        inventory_df['Codigo'] = inventory_df['Codigo'].astype(str)
        inventory_df.rename(columns={'Quantidade': 'Quantidade_Inv', 'Codigo': 'Codigo_Inv'}, inplace=True)

        prodf_df.rename(columns={'CodPF': 'CodPF_Prod', 'CodPP': 'CodPP_Prod'}, inplace=True)
        print("---- Renamed Cols:")
        print("inventory_df")
        print(inventory_df)
        print(f"inventory_df shape: {inventory_df.shape}")
        print("---- Renamed Cols:")
        print("prodf_df")
        print(prodf_df)
        print(f"prodf_df shape: {prodf_df.shape}")

        # Match 'Codigo_Inv' to T_ProdF['CodPF_Prod'] to get T_ProdF['CodPP_Prod'] as 'CodPP'
        inventory_df = pd.merge(inventory_df, prodf_df[['CodPF_Prod', 'CodPP_Prod']], left_on='Codigo_Inv', right_on='CodPF_Prod', how='left')
        print("---- Matched 1 Cols:")
        print("inventory_df")
        print(inventory_df)
        print(f"inventory_df shape: {inventory_df.shape}")

        # Create UCP by matching CodPP_Prod to T_Entradas[Pai], but exclude rows where Pai is blank
        filtered_entradas_with_pai = filtered_entradas[filtered_entradas['Pai'].notna() & (filtered_entradas['Pai'] != '')]

        duplicate_pai = filtered_entradas_with_pai[filtered_entradas_with_pai.duplicated(subset=['Pai'], keep=False)]
        print("Duplicate Pai values in filtered_entradas_with_pai:")
        print(duplicate_pai)
        if not duplicate_pai.empty:
            print("Warning: Duplicate 'Pai' values found in T_Entradas:")
            print(duplicate_pai)
            print("\nPlease correct the file to ensure unique 'Pai' values.")
            sys.exit("Execution stopped due to duplicate 'Pai' values.")

        # Now perform the merge only with non-blank 'Pai' values
        inventory_df = pd.merge(inventory_df,
                                filtered_entradas_with_pai[['Pai', 'Ult CU R$']].rename(columns={'Ult CU R$': 'UCP'}),
                                left_on='CodPP_Prod',
                                right_on='Pai',
                                how='left')
        print("---- Create UCP by matching CodPP_Prod to T_Entradas[Pai] (non-blank Pai)")
        print(inventory_df)
        print(f"inventory_df shape after merge: {inventory_df.shape}")

        # Create UCF by matching Codigo_Inv to T_Entradas[Filho]
        inventory_df = pd.merge(inventory_df, filtered_entradas[['Filho', 'Ult CU R$']].rename(columns={'Ult CU R$': 'UCF'}),
                                left_on='Codigo_Inv', right_on='Filho', how='left')
        print("---- Create UCP by matching CodPP_Prod to T_Entradas[Filho]")
        print("inventory_df")
        print(inventory_df)
        print(f"inventory_df shape: {inventory_df.shape}")

        # Ensure 'Quantidade_Inv', 'UCP', and 'UCF' are numeric (force conversion)
        inventory_df['Quantidade_Inv'] = pd.to_numeric(inventory_df['Quantidade_Inv'], errors='coerce').fillna(0)
        inventory_df['UCP'] = pd.to_numeric(inventory_df['UCP'], errors='coerce').fillna(0)
        inventory_df['UCF'] = pd.to_numeric(inventory_df['UCF'], errors='coerce').fillna(0)

        # Create UCU: If UCP > 0, use UCP; otherwise, use UCF
        inventory_df['UCU'] = inventory_df.apply(lambda row: row['UCP'] if row['UCP'] > 0 else row['UCF'], axis=1)

        # Create UCT: UCU * Quantidade_Inv
        inventory_df['UCT'] = inventory_df['UCU'] * inventory_df['Quantidade_Inv']

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

            print(f"Processing data for year {year}, month {month:02d}")

            # Step 1: Process and stack inventory data for the given year and month
            inventory_df = process_inventory_files(year, month)
            if inventory_df is None:
                continue

            # Step 2: Lookup CU values and calculate UCU and UCT
            final_df = lookup_cu_values(inventory_df)
            if final_df is None:
                continue

            # Step 3: Save the resulting dataframe to a new Excel file
            output_filepath = os.path.join(base_dir, 'clean', f'combined_inventory_{year}_{month:02d}.xlsx')
            final_df.to_excel(output_filepath, index=False)
            print(f"Saved combined inventory data for {year}-{month:02d} to {output_filepath}")


if __name__ == "__main__":
    process_all_months()
