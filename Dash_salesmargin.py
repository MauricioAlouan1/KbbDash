from dash import dcc, html

# Define the sales & margin layout
sales_margin_layout = html.Div([
    html.H2("Sales & Margin per Product"),
    dcc.Graph(id='sales-and-margin'),
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

# Add callback for sales & margin view
