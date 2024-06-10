import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px
import numpy as np  # Make sure to import numpy

# Sample Data
df = pd.DataFrame({
    "Date": pd.date_range(start='1/1/2020', periods=100),
    "Value": (np.random.rand(100) * 100).round(2)
})

# Create a Dash application
app = dash.Dash(__name__)

# Define the layout of the application
app.layout = html.Div([
    dcc.Graph(id='line-chart'),
    dcc.Dropdown(
        id='dropdown',
        options=[{'label': x, 'value': x} for x in df['Date'].dt.year.unique()],
        value=df['Date'].dt.year.unique()[0]
    )
])

# Callback to update graph based on dropdown
@app.callback(
    Output('line-chart', 'figure'),
    [Input('dropdown', 'value')]
)
def update_chart(selected_year):
    filtered_df = df[df['Date'].dt.year == selected_year]
    fig = px.line(filtered_df, x='Date', y='Value', title=f'Yearly Data for {selected_year}')
    return fig

# Run the application
if __name__ == '__main__':
    app.run_server(debug=True)
