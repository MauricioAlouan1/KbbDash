import fitz  # PyMuPDF
import pandas as pd

def extract_data_from_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    data = []

    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        text = page.get_text("text")
        data.append(text)

    return data

def process_extracted_data(extracted_data):
    rows = []
    for page in extracted_data:
        lines = page.split("\n")
        for line in lines:
            if "Saldo Atual" in line:
                continue
            parts = line.split()
            if len(parts) > 1:
                rows.append(parts)

    columns = ["Código", "Descrição", "Classificação", "Saldo Atual", "Saldo Anterior", "Débito", "Crédito"]
    df = pd.DataFrame(rows, columns=columns)
    return df

def integrate_data(existing_data, new_data):
    combined_data = pd.concat([existing_data, new_data], ignore_index=True)
    return combined_data

def main():
    pdf_path = "/mnt/data/Balancete Kavod 06.2024.pdf"
    extracted_data = extract_data_from_pdf(pdf_path)
    processed_data = process_extracted_data(extracted_data)
    
    # Placeholder for your actual existing data
    existing_data = pd.DataFrame()  # Replace with your actual existing data loading logic
    final_data = integrate_data(existing_data, processed_data)
    
    # Save or further process `final_data` as needed
    print(final_data.head())

if __name__ == "__main__":
    main()
