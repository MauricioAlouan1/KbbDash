import pandas as pd
import re

def parse_text_to_excel(text_file_path, excel_output_path):
    """Parses extracted text and saves transactions to an Excel file."""
    transactions = []

    # Regex patterns for parsing transactions
    transaction_pattern_full = re.compile(r"^(\d{2}-\d{2}-\d{4})\s+(.+?)\s+(\d+)\s+R\$ ([\d.,-]+)\s+R\$ ([\d.,-]+)$")
    transaction_pattern_simple = re.compile(r"^(\d{2}-\d{2}-\d{4})\s+(\d+)\s+R\$ ([\d.,-]+)\s+R\$ ([\d.,-]+)$")

    # Read text file
    with open(text_file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()

        # Match full transaction format
        match_full = transaction_pattern_full.match(line)
        if match_full:
            date, description, transaction_id, value, balance = match_full.groups()
        else:
            # Match simple transaction format (no description)
            match_simple = transaction_pattern_simple.match(line)
            if match_simple:
                date, transaction_id, value, balance = match_simple.groups()
                description = "N/A"  # Assign "N/A" when description is missing
            else:
                continue  # Skip lines that do not match

        try:
            value = float(value.replace(".", "").replace(",", "."))
            balance = float(balance.replace(".", "").replace(",", "."))
            transactions.append([date, description, transaction_id, value, balance])
        except ValueError:
            continue  # Skip malformed data

    # Convert to DataFrame
    df = pd.DataFrame(transactions, columns=["Data", "Descrição", "ID da operação", "Valor", "Saldo"])

    # Save to Excel
    df.to_excel(excel_output_path, index=False)
    print(f"Parsed transactions saved to: {excel_output_path}")

# Usage
text_file_path = "/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/raw/2025_01/MLK_Extrato_2025_01.txt"
excel_output_path = "/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/clean/2025_01/MLK_Extrato_2025_01.xlsx"
parse_text_to_excel(text_file_path, excel_output_path)
