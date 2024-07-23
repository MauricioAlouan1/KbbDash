# shared.py
import os
import pandas as pd
from dash import Dash

app = Dash(__name__, suppress_callback_exceptions=True)

# Global variable to store loaded data
loaded_data = None

# Function to load data
def load_data():
    global loaded_data
    if loaded_data is not None:
        return loaded_data

    path_options = [
        '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/clean/merged_data.xlsx',
        '/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/clean/merged_data.xlsx'
    ]
    for path in path_options:
        if os.path.exists(path):
            data_path = path
            break
    else:
        print("None of the specified directories exist.")
        return None

    # Read all sheets from the Excel file into a dictionary of dataframes
    try:
        loaded_data = pd.read_excel(data_path, sheet_name=None)
        print(f"Loaded data from {data_path}")
        print("Sheet names:", list(loaded_data.keys()))
        return loaded_data
    except Exception as e:
        print(f"Error reading data: {e}")
        return None
