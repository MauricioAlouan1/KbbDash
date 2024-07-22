from dash import dcc, html

# Define the overview layout
overview_layout = html.Div([
    html.H2("Overview"),
    # Add your overview components here, e.g., summary cards
    html.Div([
        html.Div("Sales: 71163.13"),
        html.Div("Profit: 11635.031250760063"),
        html.Div("Profit to Sales Ratio: 16.35%"),
        html.Div("Number of Products: 67"),
        html.Div("Number of Returns: 0"),
        html.Div("Number of Sold Products: 531")
    ])
])
