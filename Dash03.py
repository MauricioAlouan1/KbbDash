import os
import pandas as pd
from dash import Dash, dcc, html
import dash_table
import plotly.express as px
import plotly.graph_objects as go
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
    print("None of the specified directories exist.")
    base_dir = None

print("Base directory set to:", base_dir)

# Load your processed data
if base_dir:
    data_file = os.path.join(base_dir, 'clean/merged_data.xlsx')
    data = pd.read_excel(data_file, sheet_name=None)
    print("Sheets loaded:", data.keys())
else:
    print("Data file not found. Please check the directories.")
    data = {}

# Initialize the Dash app
app = Dash(__name__)

# Create a layout for the app
app.layout = html.Div([
    html.H1("Sales Dashboard", style={'textAlign': 'center'}),
    dcc.Dropdown(
        id='sheet-dropdown',
        options=[{'label': sheet, 'value': sheet} for sheet in data.keys()],
        value=list(data.keys())[0]
    ),
    html.Div([
        html.Div([
            html.H2(id='total-sales'),
            html.H2(id='total-profit'),
            html.H2(id='profit-to-sales-ratio'),
            html.H2(id='number-of-orders'),
            html.H2(id='number-of-returns'),
            html.H2(id='number-of-sold-products'),
        ], className='four columns'),
        html.Div([
            dcc.Graph(id='line-chart'),
            dcc.Graph(id='sales-diff-chart'),
        ], className='eight columns'),
    ], className='row'),
    html.Div([
        html.Div([
            dcc.Graph(id='category-sales-chart'),
        ], className='four columns'),
        html.Div([
            dcc.Graph(id='subcategory-sales-chart'),
        ], className='four columns'),
        html.Div([
            dcc.Graph(id='shipping-cost-chart'),
        ], className='four columns'),
    ], className='row'),
    html.Div([
        html.Div([
            dcc.Graph(id='profit-to-sales-ratio-chart'),
        ], className='six columns'),
        html.Div([
            dcc.Graph(id='top-products-chart'),
        ], className='six columns'),
    ], className='row'),
    html.Div(id='additional-graphs')
])

# Define callback to update table based on selected sheet
@app.callback(
    Output('total-sales', 'children'),
    Output('total-profit', 'children'),
    Output('profit-to-sales-ratio', 'children'),
    Output('number-of-orders', 'children'),
    Output('number-of-returns', 'children'),
    Output('number-of-sold-products', 'children'),
    Output('line-chart', 'figure'),
    Output('sales-diff-chart', 'figure'),
    Output('category-sales-chart', 'figure'),
    Output('subcategory-sales-chart', 'figure'),
    Output('shipping-cost-chart', 'figure'),
    Output('profit-to-sales-ratio-chart', 'figure'),
    Output('top-products-chart', 'figure'),
    Output('additional-graphs', 'children'),
    Input('sheet-dropdown', 'value')
)
def update_dashboard(selected_sheet):
    df = data[selected_sheet]

    # Calculate metrics
    total_sales = df['VLRTOTALPSKU'].sum() if 'VLRTOTALPSKU' in df.columns else 0
    total_profit = df['MARGVLR'].sum() if 'MARGVLR' in df.columns else 0
    profit_to_sales_ratio = (total_profit / total_sales * 100) if total_sales != 0 else 0
    number_of_products = df['CODPP'].nunique() if 'CODPP' in df.columns else 0
    number_of_returns = df[df['STATUS PEDIDO'] == 'CANCELADO'].shape[0] if 'STATUS PEDIDO' in df.columns else 0
    number_of_sold_products = df['QTD'].sum() if 'QTD' in df.columns else 0

    # Line chart for time series data (using ANOMES)
    line_fig = px.line(df, x='ANOMES', y='VLRTOTALPSKU', title='Sales Over Time') if 'ANOMES' in df.columns else {}

    # Sales difference chart
    sales_diff_fig = go.Figure()
    if 'ANOMES' in df.columns:
        df['SALES_DIFF'] = df['VLRTOTALPSKU'].diff()
        sales_diff_fig = px.bar(df, x='ANOMES', y='SALES_DIFF', title='Sales Difference Over Time')

    # Category sales chart
    category_sales_fig = px.bar(df, x='CATEGORIA', y='VLRTOTALPSKU', title='Sales by Category') if 'CATEGORIA' in df.columns else {}

    # Subcategory sales chart
    subcategory_sales_fig = px.bar(df, x='SUBCATEGORIA', y='VLRTOTALPSKU', title='Sales by Subcategory') if 'SUBCATEGORIA' in df.columns else {}

    # Shipping cost chart
    shipping_cost_fig = px.bar(df, x='CATEGORIA', y='FRETEVLR', title='Shipping Cost by Category') if 'FRETEVLR' in df.columns else {}

    # Profit to sales ratio chart
    profit_to_sales_ratio_fig = px.scatter(df, x='VLRTOTALPSKU', y='MARGVLR', size='QTD', color='CATEGORIA', title='Profit to Sales Ratio') if 'CATEGORIA' in df.columns else {}

    # Top products chart
    top_products_fig = px.bar(df.nlargest(15, 'VLRTOTALPSKU'), x='CODPP', y='VLRTOTALPSKU', title='Top 15 Products by Sales')

    additional_graphs = []

    # Specific visualizations for MLK_Vendas
    if selected_sheet == 'MLK_Vendas':
        # Sales per CODPP
        sales_fig = px.bar(df, x='CODPP', y='VLRTOTALPSKU', title='Sales per CODPP')
        additional_graphs.append(dcc.Graph(figure=sales_fig))

        # Margin per CODPP
        if 'MARGVLR' in df.columns:
            margin_fig = px.bar(df, x='CODPP', y='MARGVLR', title='Margin per CODPP')
            additional_graphs.append(dcc.Graph(figure=margin_fig))

    return (
        f"Sales: {total_sales}",
        f"Profit: {total_profit}",
        f"Profit to Sales Ratio: {profit_to_sales_ratio:.2f}%",
        f"Number of Products: {number_of_products}",
        f"Number of Returns: {number_of_returns}",
        f"Number of Sold Products: {number_of_sold_products}",
        line_fig,
        sales_diff_fig,
        category_sales_fig,
        subcategory_sales_fig,
        shipping_cost_fig,
        profit_to_sales_ratio_fig,
        top_products_fig,
        additional_graphs
    )

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
