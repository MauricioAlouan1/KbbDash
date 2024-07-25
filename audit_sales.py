import pandas as pd
import os

def read_monthly_data(base_dir, client_name):
    # List of months you have data for
    months = ['2024_01', '2024_02', '2024_03', '2024_04', '2024_05', '2024_06']
    
    # Initialize an empty list to store data from all months
    all_data = []
    
    # Loop through each month and read the relevant file
    for month in months:
        file_path = os.path.join(base_dir, month, f'O_NFCI_{month}_clean.xlsx')
        if os.path.exists(file_path):
            df = pd.read_excel(file_path)
            # Filter the data for the specific client
            client_data = df[df['Cliente (Nome Fantasia)'] == client_name]
            all_data.append(client_data)
        else:
            print(f"File {file_path} does not exist.")
    
    # Concatenate all the monthly data into a single DataFrame
    all_data_df = pd.concat(all_data, ignore_index=True)
    return all_data_df

def audit_sales(client_data):
    # Calculate Total Cost (Total de Mercadoria + taxes)
    client_data['Total Cost'] = client_data['Total de Mercadoria'] + client_data['Valor do ICMS ST'] + client_data['Valor do IPI']
    
    # Calculate Profit Margin
    client_data['Profit Margin'] = (client_data['Total de Mercadoria'] - client_data['Total Cost']) / client_data['Total Cost']
    
    # Group by Product Code to summarize
    summary = client_data.groupby('Código do Produto').agg({
        'Quantidade': 'sum',
        'Preco Calc': 'mean',
        'Total de Mercadoria': 'sum',
        'Total Cost': 'sum',
        'Profit Margin': 'mean',
        'Data de Emissão (completa)': ['min', 'max']
    }).reset_index()
    
    # Flatten the MultiIndex columns
    summary.columns = ['_'.join(col).strip() for col in summary.columns.values]
    
    return summary

def main():
    base_dir = '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/clean'
    client_name = 'ALWE'
    
    # Step 1: Read data
    client_data = read_monthly_data(base_dir, client_name)
    
    if client_data.empty:
        print("No data found for the specified client.")
        return
    
    # Step 2: Audit sales
    summary = audit_sales(client_data)
    
    # Step 3: Save the summary to an Excel file
    output_path = os.path.join(base_dir, 'audit_summary.xlsx')
    summary.to_excel(output_path, index=False)
    print(f"Audit summary saved to {output_path}")

if __name__ == "__main__":
    main()
