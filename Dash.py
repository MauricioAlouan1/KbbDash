import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc

# Import views
from Dash_overview import overview_layout
from Dash_sheetview import sheet_view_layout
from Dash_salesmargin import sales_margin_layout

# Initialize the app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

# Define the layout with navigation
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    dbc.NavbarSimple(
        children=[
            dbc.NavItem(dbc.NavLink("Overview", href="/")),
            dbc.NavItem(dbc.NavLink("Sheet View", href="/sheet-view")),
            dbc.NavItem(dbc.NavLink("Sales & Margin", href="/sales-margin")),
        ],
        brand="Dashboard",
        brand_href="/",
        color="primary",
        dark=True,
    ),
    html.Div(id='page-content')
])

# Define callback to update page content
@app.callback(Output('page-content', 'children'),
              Input('url', 'pathname'))
def display_page(pathname):
    if pathname == '/sheet-view':
        return sheet_view_layout
    elif pathname == '/sales-margin':
        return sales_margin_layout
    else:
        return overview_layout

if __name__ == '__main__':
    app.run_server(debug=True)
