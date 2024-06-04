import dash
from dash import html, dcc, Input, Output
import pandas as pd
import plotly.express as px  # For easier creation of plots

# Sample DataFrame
df = pd.DataFrame({
    'A': ['foo', 'bar', 'foo', 'bar', 'foo', 'bar', 'foo', 'foo'],
    'B': ['one', 'one', 'two', 'three', 'two', 'two', 'one', 'three'],
    'C': range(8),
    'D': [i * 2 for i in range(8)]  # Using list comprehension to multiply each element
})

app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Graph(id='graph-with-slider'),
    dcc.Slider(
        id='year-slider',
        min=df['C'].min(),
        max=df['C'].max(),
        value=df['C'].min(),
        marks={str(year): str(year) for year in df['C'].unique()},
        step=None
    )
])

@app.callback(
    Output('graph-with-slider', 'figure'),
    Input('year-slider', 'value'))
def update_figure(selected_year):
    filtered_df = df[df.C == selected_year]
    fig = px.scatter(filtered_df, x='A', y='D', color='B', size='D',
                     hover_data=['B'], size_max=55)
    fig.update_layout(transition_duration=500)
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)
