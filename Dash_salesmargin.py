from dash import html, dcc, Input, Output
import plotly.express as px
import pandas as pd
from dash import app, load_data

# Define the layout for the sales and margin view
salesmargin_layout = html.Div([
    html.H2("Sales and Margin per Product"),
    html.Div(
        style={'display': 'flex', 'justify-content': 'space-between', 'padding': '10px 0'},
        children=[
            dcc.DatePickerRange(
                id='date-picker',
                start_date_placeholder_text='Start Date',
                end_date_placeholder_text='End Date',
            ),
            dcc.Dropdown(
                id='company-filter',
                options=[
                    {'label': 'Company A', 'value': 'A'},
                    {'label': 'Company B', 'value': 'B'},
                    {'label': 'Company K', 'value': 'K'},
                ],
                placeholder='Select Company'
            ),
            dcc.Dropdown(
                id='marketplace-filter',
                options=[
                    {'label': 'Marketplace ML', 'value': 'ML'},
                    {'label': 'Marketplace MA', 'value': 'MA'},
                    {'label': 'Marketplace MB', 'value': 'MB'},
                ],
                placeholder='Select Marketplace'
            )
        ]
    ),
    dcc.Graph(id='sales-margin-graph'),
    dcc.Slider(
        id='page-slider',
        min=1,
        max=1,
        step=1,
        value=1,
        marks={1: '1'}
    ),
])

# Define the callback for the sales and margin graph
@app.callback(
    Output('sales-margin-graph', 'figure'),
    [Input('date-picker', 'start_date'),
     Input('date-picker', 'end_date'),
     Input('company-filter', 'value'),
     Input('marketplace-filter', 'value'),
     Input('page-slider', 'value')]
)
def update_sales_margin_graph(start_date, end_date, company, marketplace, page):
    df = load_data()['MLK_Vendas']  # Adjust the key as needed

    # Filter by date range
    if start_date and end_date:
        mask = (df['DATA DA VENDA'] >= start_date) & (df['DATA DA VENDA'] <= end_date)
        df = df.loc[mask]

    # Filter by company
    if company:
        df = df[df['EMPRESA'] == company]

    # Filter by marketplace
    if marketplace:
        df = df[df['MP'] == marketplace]

    # Group and paginate
    grouped_df = df.groupby('CODPP').agg({
        'VLRTOTALPSKU': 'sum',
        'MARGVLR': 'sum'
    }).reset_index()
    grouped_df = grouped_df.sort_values(by='VLRTOTALPSKU', ascending=False)
    grouped_df['MARGPCT'] = (grouped_df['MARGVLR'] / grouped_df['VLRTOTALPSKU']) * 100

    page_size = 10
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    paginated_df = grouped_df.iloc[start_idx:end_idx]

    fig = px.bar(paginated_df, x='VLRTOTALPSKU', y='CODPP', orientation='h',
                 hover_data={'MARGVLR': True, 'MARGPCT': ':.2f'},
                 labels={'VLRTOTALPSKU': 'Sales (R$)', 'MARGVLR': 'Margin (R$)', 'MARGPCT': 'Margin (%)'})

    fig.update_layout(yaxis={'categoryorder': 'total ascending'}, barmode='group')
    return fig
