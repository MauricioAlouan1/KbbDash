import pandas as pd
import shutil
from openpyxl import load_workbook

# Define paths
source_file = "/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/clean/2025_01/R_Resumo_2025_01.xlsx"
template_file = "/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/Template/PivotTemplate.xlsm"
output_file = "/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/clean/2025_01/Pivot_Report_2025_01.xlsm"

# Step 1: Copy the template (preserves macros)
shutil.copy(template_file, output_file)
print(f"✅ Copied template to {output_file}")

# Step 2: Open the template workbook with macros
print("✅ Opening template with macros...")
wb_template = load_workbook(output_file, keep_vba=True)

# Step 3: Open the source workbook
print("✅ Loading source file into memory...")
wb_source = load_workbook(source_file)

# Step 4: Remove all existing sheets from the template
print(f"✅ Removing {len(wb_template.sheetnames)} sheets from template...")
for sheet in wb_template.sheetnames:
    del wb_template[sheet]

# Step 5: Copy all sheets from source to template
print(f"✅ Copying {len(wb_source.sheetnames)} sheets...")
for sheet_name in wb_source.sheetnames:
    print(f"📄 Copying sheet: {sheet_name}")
    source_sheet = wb_source[sheet_name]

    # Create a new sheet in the template with the same name
    new_sheet = wb_template.create_sheet(title=sheet_name)

    # Copy data from the source sheet to the new sheet
    for row in source_sheet.iter_rows():
        for cell in row:
            new_sheet[cell.coordinate].value = cell.value

# Step 6: Save and close the updated workbook
wb_template.save(output_file)
wb_template.close()
wb_source.close()

print(f"✅ Final file saved at {output_file}")
print("📌 Open the file in Excel and run the macro: 'CreatePivotTable'.")
