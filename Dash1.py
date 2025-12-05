# Dash1.py (previously Dash.py)
from dash import dcc, html, Input, Output, dash_table
import pandas as pd
from pandas.tseries.offsets import MonthEnd
from Dash_overview import overview_layout
from Dash_sheetview import sheetview_layout
from Dash_salesmargin import salesmargin_layout
from Dash_commands import commands_layout
from Dash_shared import app, load_data

df_init = load_data()

def _default_filters_from_df(data):
    # If data is a dict, try to pick a relevant sheet
    df = None
    if isinstance(data, dict):
        # Prioritize sheets that likely have the relevant columns (DATE, EMPRESA, MP)
        for key in ["MLK_Vendas", "O_NFCI", "Kon_RelGeral", "L_LPI"]:
            if key in data and not data[key].empty:
                df = data[key]
                break
        # Fallback to first non-empty sheet
        if df is None:
            for key, val in data.items():
                if not val.empty:
                    df = val
                    break
    else:
        df = data

    # Safe fallbacks
    default = {
        "start": pd.Timestamp.today().normalize() - MonthEnd(1) + pd.offsets.MonthBegin(0),
        "end":   (pd.Timestamp.today().normalize() - MonthEnd(1)) + MonthEnd(0),
        "emp_options": [],
        "mp_options": [],
    }
    if df is None or df.empty or "DATE" not in df.columns:
        return default

    # Ensure datetime
    d = pd.to_datetime(df["DATE"], errors="coerce").dropna()
    if d.empty:
        return default

    # Pick **last full month** present in the data (by max DATE)
    last_date = d.max()               # e.g., 2025-08-31
    last_month_start = last_date.replace(day=1)
    last_month_end   = (last_month_start + MonthEnd(0))

    # Build options
    emp = sorted(df.get("EMPRESA", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
    mp  = sorted(df.get("MP", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())

    return {
        "start": last_month_start,
        "end":   last_month_end,
        "emp_options": [{"label": e, "value": e} for e in emp],
        "mp_options":  [{"label": m, "value": m} for m in mp],
    }

_defaults = _default_filters_from_df(df_init)



# Define the filter components
# Replace your DatePickerRange & dropdowns with:

date_picker = dcc.DatePickerRange(
    id="date-picker",
    start_date=_defaults["start"].date(),
    end_date=_defaults["end"].date(),
    display_format="YYYY-MM-DD",
)

company_filter = dcc.Dropdown(
    id="company-filter",
    options=_defaults["emp_options"],
    value=None,              # None = no filter
    multi=True,
    placeholder="Select Company",
)

marketplace_filter = dcc.Dropdown(
    id="marketplace-filter",
    options=_defaults["mp_options"],
    value=None,              # None = no filter
    multi=True,
    placeholder="Select Marketplace",
)

# Define the main layout with tabs
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    dcc.Tabs(id="tabs", value='overview', children=[
        dcc.Tab(label='Overview', value='overview'),
        dcc.Tab(label='Sheet View', value='sheetview'),
        dcc.Tab(label='Sales & Margin', value='salesmargin'),
        dcc.Tab(label='Commands', value='commands')
    ]),
    html.Div(
        style={'display': 'flex', 'justify-content': 'space-between', 'padding': '10px 0'},
        children=[
            html.Div(date_picker, style={'flex': '1', 'margin-right': '10px'}),
            html.Div(company_filter, style={'flex': '1', 'margin-right': '10px'}),
            html.Div(marketplace_filter, style={'flex': '1', 'margin-right': '10px'})
        ]
    ),
    html.Div(id='tabs-content')
])

# Callback to render tab content
@app.callback(Output('tabs-content', 'children'),
              [Input('tabs', 'value')])

def render_content(tab):
    if tab == 'overview':
        return overview_layout
    elif tab == 'sheetview':
        data = load_data()
        if data:
            sheet_options = [{'label': sheet, 'value': sheet} for sheet in data.keys()]
        else:
            sheet_options = []
        return html.Div([
            dcc.Dropdown(
                id='sheet-selector',
                options=sheet_options,
                value=sheet_options[0]['value'] if sheet_options else None
            ),
            html.Div(id='sheet-content')
        ])
    elif tab == 'salesmargin':
        return salesmargin_layout
    elif tab == 'commands':
        return commands_layout
    return overview_layout

# Callback to update sheet content
@app.callback(Output('sheet-content', 'children'),
              [Input('sheet-selector', 'value')])
def update_sheet_content(selected_sheet):
    data = load_data()
    if data and selected_sheet:
        df = data[selected_sheet]
        return dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df.columns],
            style_table={'overflowX': 'auto'}
        )
    return html.Div(['Select a sheet to view its content.'])

@app.callback(
    Output('overview-content', 'children'),
    [Input('date-picker', 'start_date'),
     Input('date-picker', 'end_date'),
     Input('company-filter', 'value'),
     Input('marketplace-filter', 'value')]
)
def update_overview_totals(start_date, end_date, company, marketplace):
    all_data = load_data()  # Load the dataset
    if all_data is None:
        return [html.H4("Error loading data.")]

    df = all_data.get('MLK_Vendas', pd.DataFrame())  # Adjust the key as needed

    if df.empty:
        return [html.H4("No data available.")]

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

    # Calculate the summary statistics
    total_sales = df['VLRTOTALPSKU'].sum()
    total_profit = df['MARGVLR'].sum()
    profit_to_sales_ratio = (total_profit / total_sales) * 100 if total_sales != 0 else 0
    number_of_products = df['CODPP'].nunique()
    number_of_returns = df[df['STATUS'] == 'DEVOLVIDO'].shape[0]
    number_of_sold_products = df['SKU'].count()

    # Create the summary display
    summary_display = [
        html.H4(f"Sales: R$ {total_sales:,.2f}"),
        html.H4(f"Profit: R$ {total_profit:,.2f}"),
        html.H4(f"Profit to Sales Ratio: {profit_to_sales_ratio:.2f}%"),
        html.H4(f"Number of Products: {number_of_products}"),
        html.H4(f"Number of Returns: {number_of_returns}"),
        html.H4(f"Number of Sold Products: {number_of_sold_products}")
    ]

    return summary_display

# Define callback for updating graphs with filters
@app.callback(
    Output('main-graph', 'figure'),
    [Input('date-picker', 'start_date'),
     Input('date-picker', 'end_date'),
     Input('company-filter', 'value'),
     Input('marketplace-filter', 'value')]
)
def update_graph(start_date, end_date, selected_companies, selected_marketplaces):
    df = load_data()['MLK_Vendas']  # Adjust the key as needed
    # Apply filters
    if start_date and end_date:
        df = df[(df['DATA DA VENDA'] >= start_date) & (df['DATA DA VENDA'] <= end_date)]
    if selected_companies:
        df = df[df['EMPRESA'].isin(selected_companies)]
    if selected_marketplaces:
        df = df[df['MP'].isin(selected_marketplaces)]
    # Update your graph creation logic here
    return create_main_graph(df)

def create_main_graph(df):
    # Define your graph creation logic here
    pass

if __name__ == '__main__':
    app.run_server(debug=True)  
