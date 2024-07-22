import os
import pandas as pd
from dash import Dash, dcc, html
import dash_table
from dash.dependencies import Input, Output

# Define the potential base directories
path_options = [
    '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/',
    '/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data'
]

# Iterate over the list and set base_dir to the first existing path
for path in path_options:
    if os.path.exists(path):
        base_dir = path
        break
else:
    # If no valid path is found, raise an error or handle it appropriately
    print("None of the specified directories exist.")
    base_dir = None  # Or set a default path if appropriate

print("Base directory set to:", base_dir)

# Load your processed data
if base_dir:
    data_file = os.path.join(base_dir, 'clean/merged_data.xlsx')
    data = pd.read_excel(data_file, sheet_name=None)

    # Print the names of the sheets to verify
    print("Sheets loaded:", data.keys())
else:
    print("Data file not found. Please check the directories.")

# Initialize the Dash app
app = Dash(__name__)

# Create a layout for the app
app.layout = html.Div([
    dcc.Dropdown(
        id='sheet-dropdown',
        options=[{'label': sheet, 'value': sheet} for sheet in data.keys()],
        value=list(data.keys())[0]
    ),
    dash_table.DataTable(id='table')
])

# Define callback to update table based on selected sheet
@app.callback(
    Output('table', 'data'),
    Output('table', 'columns'),
    Input('sheet-dropdown', 'value')
)
def update_table(selected_sheet):
    df = data[selected_sheet]
    return df.to_dict('records'), [{"name": i, "id": i} for i in df.columns]

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
