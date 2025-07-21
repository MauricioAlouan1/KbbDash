import os
import xml.etree.ElementTree as ET
import pandas as pd
import re
from openpyxl import load_workbook

# Run processing for all series for a specific month
YEAR = "2025"
MONTH = "6-Junho"

# Define base folder and available series
BASE_FOLDER = "/Users/mauricioalouan/Dropbox/nfs"
SERIES_LIST = [
    "Serie 1 - Omie",
    "Serie 2 - filial",
    "Serie 3 - Bling",
    "Serie 4 - Lexos",
    "Serie 5 - Olist",
    "Serie 6 - Meli",
    "Serie 7 - Amazon",
    "Serie 8 - Magalu Full"
]

def process_series(month, year, series):
    """Process XML invoices for a given month, year, and series."""

    folder_path = os.path.join(BASE_FOLDER, year, series, month)
    output_file = os.path.join(BASE_FOLDER, f"Extracted_Data_{year}_{month.replace('/', '-')}_{series}.xlsx")

    if not os.path.exists(folder_path):
        print(f"Skipping {series}: Folder not found -> {folder_path}")
        return

    data_list = []

    # Iterate through XML files in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith(".xml"):
            file_path = os.path.join(folder_path, filename)

            try:
                # Parse XML
                tree = ET.parse(file_path)
                root = tree.getroot()

                # Extract namespace dynamically
                namespace = {"ns": root.tag.split("}")[0].strip("{")}

                # Extract necessary fields with fallback values
                date = root.find(".//ns:ide/ns:dhEmi", namespace)
                date = date.text[:10] if date is not None else "N/A"

                nf = root.find(".//ns:ide/ns:nNF", namespace)
                nf = nf.text if nf is not None else "N/A"

                natureza = root.find(".//ns:ide/ns:natOp", namespace)
                natureza = natureza.text if natureza is not None else "N/A"

                serie = root.find(".//ns:ide/ns:serie", namespace)
                serie = serie.text if serie is not None else "N/A"

                client = root.find(".//ns:dest/ns:xNome", namespace)
                client = client.text if client is not None else "N/A"

                cpf = root.find(".//ns:dest/ns:CPF", namespace)
                cpf = cpf.text if cpf is not None else "N/A"

                pedido = root.find(".//ns:compra/ns:xPed", namespace)
                pedido = pedido.text if pedido is not None else "N/A"

                valor_produto = root.find(".//ns:ICMSTot/ns:vProd", namespace)
                valor_produto = round(float(valor_produto.text), 2) if valor_produto is not None else 0.00

                icms = root.find(".//ns:ICMSTot/ns:vICMS", namespace)
                icms = round(float(icms.text), 2) if icms is not None else 0.00

                st = root.find(".//ns:ICMSTot/ns:vST", namespace)
                st = round(float(st.text), 2) if st is not None else 0.00

                desconto = root.find(".//ns:ICMSTot/ns:vDesc", namespace)
                desconto = round(float(desconto.text), 2) if desconto is not None else 0.00

                frete = root.find(".//ns:ICMSTot/ns:vFrete", namespace)
                frete = round(float(frete.text), 2) if frete is not None else 0.00

                ipi = root.find(".//ns:ICMSTot/ns:vIPI", namespace)
                ipi = round(float(ipi.text), 2) if ipi is not None else 0.00

                desp_ass = root.find(".//ns:ICMSTot/ns:vOutro", namespace)
                desp_ass = round(float(desp_ass.text), 2) if desp_ass is not None else 0.00

                total_nf = root.find(".//ns:ICMSTot/ns:vNF", namespace)
                total_nf = round(float(total_nf.text), 2) if total_nf is not None else 0.00

                cprod = root.find(".//ns:det/ns:prod/ns:cProd", namespace)
                cprod = cprod.text if cprod is not None else "N/A"

                # Extract Amazon Order ID (NumPedAm) from infCpl
                inf_cpl_elem = root.find(".//ns:infAdic/ns:infCpl", namespace)
                if inf_cpl_elem is not None:
                    inf_cpl_text = inf_cpl_elem.text
                    amazon_order_match = re.search(r"Numero do pedido da compra:\s*([\d-]+)", inf_cpl_text)
                    num_ped_am = amazon_order_match.group(1) if amazon_order_match else "N/A"
                else:
                    num_ped_am = "N/A"

                # Calculate Check Field
                check_value = round(valor_produto + st - desconto + ipi + frete + desp_ass - total_nf, 2)

                # Store data in list
                data_list.append([date, nf, natureza, serie, client, cpf, pedido, num_ped_am, cprod, 
                                  valor_produto, icms, st, desconto, ipi, frete, desp_ass, total_nf, check_value])

            except Exception as e:
                print(f"Error processing file {filename}: {e}")

    # Convert to DataFrame
    df = pd.DataFrame(data_list, columns=[
        "Date", "NF", "Natureza", "Serie", "Client", "CPF", "Pedido", "NumPedAm", "CProd",
        "ValorProduto", "ICMS", "ST", "Desconto", "IPI", "Frete", "DespAss", "TotalNF", "Check"
    ])

    if df.empty:
        print(f"No valid data found for {series} - {month}/{year}. Skipping file creation.")
        return

    # Save to Excel
    df.to_excel(output_file, index=False)

    # Open the workbook and apply number formatting + autofilter
    wb = load_workbook(output_file)
    ws = wb.active

    # Set number format for columns
    num_format = "#,##0.00"  # Thousand separator + 2 decimal places
    num_columns = ["J", "K", "L", "M", "N", "O", "P", "Q", "R"]  # Corresponding columns in Excel

    for col in num_columns:
        for row in range(2, ws.max_row + 1):  # Skip header row
            ws[f"{col}{row}"].number_format = num_format

    # Apply autofilter to the header row
    ws.auto_filter.ref = ws.dimensions

    # Save the formatted workbook
    wb.save(output_file)

    print(f"Processed {series}: Extracted and formatted data saved to {output_file}")

# Call the function for all series for a given month
def process_all_series_for_month(year, month):
    """Iterate through all series and process XML files for a given month and year."""
    for series in SERIES_LIST:
        process_series(month, year, series)

process_all_series_for_month(YEAR, MONTH)
