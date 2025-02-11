import pdfplumber

def extract_pdf_to_text(pdf_path, text_output_path):
    """Extracts text from a PDF file and saves it to a text file."""
    raw_text = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                raw_text.extend(text.split("\n"))

    # Save extracted text to a file
    with open(text_output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(raw_text))
    
    print(f"Text extracted and saved to: {text_output_path}")

# Usage
pdf_path = "/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/raw/2025_01/mlk_Extrato_2025_01.pdf"  # Change to your actual file path
text_output_path = "/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/raw/2025_01/MLK_Extrato_2025_01.txt"
extract_pdf_to_text(pdf_path, text_output_path)
