import pandas as pd
import numpy as np
import plotly.express as px
import dash
from dash import dcc, html
from dash.dependencies import Input, Output

# Load your merged data
path_options = [
    '/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/clean/merged_data.xlsx',
    '/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/clean/merged_data.xlsx'
]

# Find the correct path
for path in path_options:
    try:
        df = pd.read_excel(path, sheet_name='MLK_Vendas')
        break
    except FileNotFoundError:
        df = None

if df is None:
    raise FileNotFoundError("Merged data file not found in any of the specified directories.")

# Print the first few rows of the dataframe to check the data
print(df.head())

# Initialize the Dash app
app = dash.Dash(__name__)

# Define the layout of the app
app.layout = html.Div([
    html.H1("MLK_Vendas Sales and Margin Dashboard"),
    html.Div([
        dcc.Graph(id='sales-by-product', config={'displayModeBar': False}),
        dcc.Graph(id='margin-by-product', config={'displayModeBar': False})
    ], style={'display': 'flex', 'flex-direction': 'row', 'justify-content': 'space-around'}),
    dcc.Slider(
        id='page-slider',
        min=1,
        max=1,  # This will be updated dynamically
        value=1,
        step=1,
        marks={i: str(i) for i in range(1, 2)}
    ),
    html.Div(id='page-number')
])

@app.callback(
    Output('sales-by-product', 'figure'),
    Output('margin-by-product', 'figure'),
    Output('page-slider', 'max'),
    Output('page-slider', 'marks'),
    Output('page-number', 'children'),
    Input('page-slider', 'value')
)
def update_graphs(page):
    try:
        # Exclude datetime columns for the sum operation
        numeric_df = df.select_dtypes(include=[np.number])

        # Group data by product and calculate sales and margin
        grouped_df = df.groupby('CODPP')[numeric_df.columns].sum().reset_index()
        
        # Sort by total sales
        sorted_df = grouped_df.sort_values(by='VLRTOTALPSKU', ascending=False)
        
        # Number of products per page
        products_per_page = 10
        total_pages = int(np.ceil(sorted_df.shape[0] / products_per_page))
        
        # Pagination
        start_index = (page - 1) * products_per_page
        end_index = start_index + products_per_page
        paginated_df = sorted_df.iloc[start_index:end_index]

        # Debug print statements
        print(f"Total pages: {total_pages}")
        print(f"Paginated dataframe for page {page}:")
        print(paginated_df.head())

        if paginated_df.empty:
            print("Paginated dataframe is empty.")
            return {}, {}, total_pages, {i: str(i) for i in range(1, total_pages + 1)}, f"Page {page} of {total_pages}"

        # Ensure that the expected columns are present
        if 'VLRTOTALPSKU' not in paginated_df.columns or 'MARGVLR' not in paginated_df.columns:
            print("Paginated dataframe does not contain the expected columns.")
            print(f"Available columns: {paginated_df.columns}")
            return {}, {}, total_pages, {i: str(i) for i in range(1, total_pages + 1)}, f"Page {page} of {total_pages}"
        
        # Sales by product
        sales_fig = px.bar(
            paginated_df,
            y='CODPP', x='VLRTOTALPSKU', orientation='h',
            labels={'VLRTOTALPSKU': '', 'CODPP': ''}
        )
        sales_fig.update_traces(
            text=paginated_df['VLRTOTALPSKU'].apply(lambda x: f"R$ {x:,.2f}"),
            textposition='outside'
        )
        sales_fig.update_layout(showlegend=False, yaxis={'categoryorder': 'total ascending'}, margin={'l': 200, 'r': 0, 't': 0, 'b': 0})

        # Margin by product
        margin_fig = px.bar(
            paginated_df,
            y='CODPP', x='MARGVLR', orientation='h',
            labels={'MARGVLR': '', 'CODPP': ''}
        )
        margin_fig.update_traces(
            text=paginated_df['MARGVLR'].apply(lambda x: f"R$ {x:,.2f}"),
            textposition='outside'
        )
        margin_fig.update_layout(showlegend=False, yaxis={'categoryorder': 'total ascending'}, margin={'l': 200, 'r': 0, 't': 0, 'b': 0})
        
        # Fix the value axis range
        sales_fig.update_layout(xaxis_range=[0, sorted_df['VLRTOTALPSKU'].max()])
        margin_fig.update_layout(xaxis_range=[0, sorted_df['MARGVLR'].max()])
        
        print("Figures created successfully.")
        
        return sales_fig, margin_fig, total_pages, {i: str(i) for i in range(1, total_pages + 1)}, f"Page {page} of {total_pages}"
    except Exception as e:
        print(f"Error in update_graphs callback: {e}")
        return {}, {}, 1, {i: str(i) for i in range(1, 2)}, f"Error: {e}"

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
