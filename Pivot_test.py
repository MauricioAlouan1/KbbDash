import pandas as pd
import os

# Define input & output paths
input_file = "/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/clean/2024_12/R_Resumo_2024_12.xlsx"
output_file = "/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/clean/2024_12/Pivot_Report.xlsx"

# Check if the input file exists
if not os.path.exists(input_file):
    raise FileNotFoundError(f"Error: The file {input_file} was not found!")

# Load data
df = pd.read_excel(input_file)

# Save the DataFrame to an Excel file using XlsxWriter
with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
    df.to_excel(writer, sheet_name="Data", index=False)

    # Get the workbook and sheets
    workbook = writer.book
    data_sheet = writer.sheets["Data"]

    # Create a new sheet for the Pivot Table
    pivot_sheet = workbook.add_worksheet("PivotTable")

    # Define the data range for the pivot table
    (max_row, max_col) = df.shape
    data_range = f"'Data'!A1:{chr(65 + max_col - 1)}{max_row + 1}"  # Adjusts based on column count

    # Create the Pivot Table in Excel
    pivot_sheet.add_pivot_table(
        data_range=data_range,
        location="B3",  # Pivot Table will start in B3
        name="PivotTable1",
        rows=["EMPRESA"],  # Row Fields
        columns=["MP"],  # Column Fields
        values=[
            ("VLRVENDA", "sum"),  # Sum of Sales
            ("ComissPctVlr", "sum")  # Sum of Commission
        ],
        filters=["MP"]  # Filters
    )

    # Save the workbook
    writer.close()

print(f"✅ Pivot table saved in {output_file}. Open in Excel and adjust filters manually.")
