import dash
from dash import dcc, html, Input, Output, State, callback_context
import subprocess
from datetime import datetime
from Dash_shared import app

# Layout
def get_default_year_month():
    today = datetime.now()
    # Default to previous month as per business logic usually
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1

default_year, default_month = get_default_year_month()

commands_layout = html.Div([
    html.H3("Execute Pipeline Steps"),
    
    html.Div([
        html.Label("Year:"),
        dcc.Input(id='cmd-year', type='number', value=default_year, style={'margin-right': '10px'}),
        html.Label("Month:"),
        dcc.Input(id='cmd-month', type='number', value=default_month, min=1, max=12, style={'margin-right': '10px'}),
    ], style={'margin-bottom': '20px'}),

    html.Div([
        html.Button("1. Create NFI (XML -> Excel)", id='btn-step1-nfi', n_clicks=0, style={'margin': '5px'}),
        html.Button("1. Create NF (XML -> Excel)", id='btn-step1-nf', n_clicks=0, style={'margin': '5px'}),
        html.Button("2. Aggregate NF", id='btn-step2-nf-agg', n_clicks=0, style={'margin': '5px'}),
        html.Button("2. Aggregate NFI", id='btn-step2-nfi-agg', n_clicks=0, style={'margin': '5px'}),
        html.Button("2.5. Process Data (Renames/Formats)", id='btn-step2-5', n_clicks=0, style={'margin': '5px'}),
        html.Button("3. Update Entradas", id='btn-step3', n_clicks=0, style={'margin': '5px'}),
        html.Button("4. Inventory Process", id='btn-step4', n_clicks=0, style={'margin': '5px'}),
        html.Button("5. Generate Report (Remake Dataset)", id='btn-step5', n_clicks=0, style={'margin': '5px'}),
    ], style={'display': 'flex', 'flex-wrap': 'wrap', 'margin-bottom': '20px'}),

    html.H4("Execution Output:"),
    html.Pre(id='cmd-output', style={'background-color': '#f0f0f0', 'padding': '10px', 'border': '1px solid #ccc', 'height': '300px', 'overflow-y': 'scroll', 'white-space': 'pre-wrap'})
])

# Callback
@app.callback(
    Output('cmd-output', 'children'),
    [Input('btn-step1-nfi', 'n_clicks'),
     Input('btn-step1-nf', 'n_clicks'),
     Input('btn-step2-nf-agg', 'n_clicks'),
     Input('btn-step2-nfi-agg', 'n_clicks'),
     Input('btn-step2-5', 'n_clicks'),
     Input('btn-step3', 'n_clicks'),
     Input('btn-step4', 'n_clicks'),
     Input('btn-step5', 'n_clicks')],
    [State('cmd-year', 'value'),
     State('cmd-month', 'value')]
)
def run_pipeline_step(btn1_nfi, btn1_nf, btn2_nf, btn2_nfi, btn2_5, btn3, btn4, btn5, year, month):
    ctx = callback_context

    if not ctx.triggered:
        return "Ready to execute commands."

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    step_map = {
        'btn-step1-nfi': 'step1_nfi',
        'btn-step1-nf': 'step1_nf',
        'btn-step2-nf-agg': 'step2_nf_agg',
        'btn-step2-nfi-agg': 'step2_nfi_agg',
        'btn-step2-5': 'step2_5_process_data',
        'btn-step3': 'step3_update_entradas',
        'btn-step4': 'step4_inventory',
        'btn-step5': 'step5_report'
    }

    step_name = step_map.get(button_id)
    if not step_name:
        return "Unknown button."

    if not year or not month:
        return "Error: Year and Month must be specified."

    cmd = [
        "python", "master_pipeline.py",
        "--step", step_name,
        "--year", str(year),
        "--month", str(month),
        "--force" # Force to skip dependency checks if user explicitly clicks? Or maybe not? Let's use force to ensure it runs if user asks.
    ]

    try:
        result = subprocess.run(
            cmd,
            cwd=".", # Current directory
            capture_output=True,
            text=True,
            check=False
        )
        
        output = f"Command: {' '.join(cmd)}\n\n"
        output += f"Exit Code: {result.returncode}\n\n"
        output += "STDOUT:\n" + result.stdout + "\n"
        if result.stderr:
            output += "STDERR:\n" + result.stderr + "\n"
            
        return output

    except Exception as e:
        return f"Error executing command: {str(e)}"
