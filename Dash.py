from dash import dcc, html, Input, Output
import dash
import pandas as pd
from Dash_overview import overview_layout
from Dash_sheetview import sheetview_layout
from Dash_salesmargin import salesmargin_layout

# Initialize the app
app = dash.Dash(__name__)

# Load your data
def load_data():
    path_options = [
        '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/merged_data.xlsx',
        '/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/merged_data.xlsx'
    ]
    for path in path_options:
        if os.path.exists(path):
            df = pd.read_excel(path)
            return df
    return None

# Define the filters
filters = html.Div([
    dcc.DatePickerRange(
        id='date-picker-range',
        start_date='2024-01-01',
        end_date='2024-12-31',
        display_format='YYYY-MM-DD'
    ),
    dcc.Dropdown(
        id='company-filter',
        options=[
            {'label': 'Company A', 'value': 'A'},
            {'label': 'Company K', 'value': 'K'}
        ],
        placeholder='Select a company'
    ),
    dcc.Dropdown(
        id='product-filter',
        options=[
            # Add options dynamically or manually
        ],
        placeholder='Select a product'
    )
])

# Update the layout
app.layout = html.Div([
    dcc.Tabs(id='tabs', value='overview', children=[
        dcc.Tab(label='Overview', value='overview'),
        dcc.Tab(label='Sheet View', value='sheetview'),
        dcc.Tab(label='Sales and Margin', value='salesmargin')
    ]),
    html.Div(id='tabs-content'),
    filters,
    dcc.Graph(id='main-graph')
])

@app.callback(Output('tabs-content', 'children'), [Input('tabs', 'value')])
def render_content(tab):
    if tab == 'overview':
        return overview_layout
    elif tab == 'sheetview':
        return sheetview_layout
    elif tab == 'salesmargin':
        return salesmargin_layout

@app.callback(
    Output('main-graph', 'figure'),
    [
        Input('date-picker-range', 'start_date'),
        Input('date-picker-range', 'end_date'),
        Input('company-filter', 'value'),
        Input('product-filter', 'value')
    ]
)
def update_graph(start_date, end_date, company, product):
    # Load and filter your data based on the filter inputs
    df = load_data()  # Your function to load data
    if df is None:
        return {}
    
    if start_date:
        df = df[df['date'] >= start_date]
    if end_date:
        df = df[df['date'] <= end_date]
    if company:
        df = df[df['company'] == company]
    if product:
        df = df[df['product'] == product]
    
    # Update your graph based on the filtered data
    figure = {
        'data': [
            # Your graph data here
        ],
        'layout': {
            # Your graph layout here
        }
    }
    return figure

# Additional callbacks for other components
# ...

if __name__ == '__main__':
    app.run_server(debug=True)
