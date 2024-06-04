import os
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

def parse_xml(xml_file):
    """Parse the XML file and extract necessary data."""
    tree = ET.parse(xml_file)
    root = tree.getroot()
    # Assuming XML structure, replace with actual tags
    nf_number = root.find('.//nfNumber').text
    nf_date = root.find('.//nfDate').text
    nf_value = root.find('.//nfValue').text
    return {
        'Nota Fiscal Number': nf_number,
        'Date': nf_date,
        'Value': nf_value
    }

def process_directory(base_path, current_month=True):
    series_dirs = [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]
    for series in series_dirs:
        series_path = os.path.join(base_path, series)
        month_dirs = [d for d in os.listdir(series_path) if os.path.isdir(os.path.join(series_path, d))]
        for month in month_dirs:
            if current_month and month != datetime.now().strftime("%m-%B"):
                continue
            month_path = os.path.join(series_path, month)
            xml_files = [f for f in os.listdir(month_path) if f.endswith('.xml')]
            all_data = []
            for xml_file in xml_files:
                xml_path = os.path.join(month_path, xml_file)
                data = parse_xml(xml_path)
                all_data.append(data)
            df = pd.DataFrame(all_data)
            # Save to Excel file
            df.to_excel(os.path.join(month_path, f"{series}_{month}.xlsx"), index=False)

# Example usage
base_directory = '/Users/simon/Library/CloudStorage/Dropbox/nfs/2024'
process_directory(base_directory)
