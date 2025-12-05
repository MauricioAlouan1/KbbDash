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

    # Main Button
    html.Div([
        html.Button("RUN MASTER PIPELINE (ALL STEPS)", id='btn-master', n_clicks=0, 
                    style={'font-size': '18px', 'padding': '15px', 'background-color': '#007bff', 'color': 'white', 'border': 'none', 'cursor': 'pointer', 'width': '100%', 'margin-bottom': '20px'}),
    ]),

    # Collapsible Advanced Steps
    html.Details([
        html.Summary("Advanced: Individual Pipeline Steps", style={'cursor': 'pointer', 'margin-bottom': '10px', 'font-weight': 'bold'}),
        html.Div([
            html.Button("1. Create NFI (XML -> Excel)", id='btn-step1-nfi', n_clicks=0, style={'margin': '5px'}),
            html.Button("1. Create NF (XML -> Excel)", id='btn-step1-nf', n_clicks=0, style={'margin': '5px'}),
            html.Button("2. Aggregate NF", id='btn-step2-nf-agg', n_clicks=0, style={'margin': '5px'}),
            html.Button("2. Aggregate NFI", id='btn-step2-nfi-agg', n_clicks=0, style={'margin': '5px'}),
            html.Button("2.5. Process Data (Renames/Formats)", id='btn-step2-5', n_clicks=0, style={'margin': '5px'}),
            html.Button("3. Update Entradas", id='btn-step3', n_clicks=0, style={'margin': '5px'}),
            html.Button("4. Inventory Process", id='btn-step4', n_clicks=0, style={'margin': '5px'}),
            html.Button("5. Generate Report (Remake Dataset)", id='btn-step5', n_clicks=0, style={'margin': '5px'}),
        ], style={'display': 'flex', 'flex-wrap': 'wrap', 'margin-bottom': '20px', 'padding': '10px', 'border': '1px solid #ddd'})
    ], style={'margin-bottom': '20px'}),

    # Other Scripts Section
    html.H4("Other Scripts"),
    html.Div([
        html.Button("Conciliação Estoque", id='btn-conc-estoque', n_clicks=0, style={'margin': '5px', 'background-color': '#6c757d', 'color': 'white'}),
        html.Button("Conciliação CAR Receber", id='btn-conc-car', n_clicks=0, style={'margin': '5px', 'background-color': '#6c757d', 'color': 'white'}),
        html.Button("Compras", id='btn-compras', n_clicks=0, style={'margin': '5px', 'background-color': '#6c757d', 'color': 'white'}),
        html.Button("Atualiza Entradas (Standalone)", id='btn-atualiza-entradas-std', n_clicks=0, style={'margin': '5px', 'background-color': '#6c757d', 'color': 'white'}),
    ], style={'display': 'flex', 'flex-wrap': 'wrap', 'margin-bottom': '20px'}),

    html.H4("Execution Output:"),
    html.Pre(id='cmd-output', style={'background-color': '#f0f0f0', 'padding': '10px', 'border': '1px solid #ccc', 'height': '300px', 'overflow-y': 'scroll', 'white-space': 'pre-wrap'})
])

# Callback
@app.callback(
    Output('cmd-output', 'children'),
    [Input('btn-master', 'n_clicks'),
     Input('btn-step1-nfi', 'n_clicks'),
     Input('btn-step1-nf', 'n_clicks'),
     Input('btn-step2-nf-agg', 'n_clicks'),
     Input('btn-step2-nfi-agg', 'n_clicks'),
     Input('btn-step2-5', 'n_clicks'),
     Input('btn-step3', 'n_clicks'),
     Input('btn-step4', 'n_clicks'),
     Input('btn-step5', 'n_clicks'),
     Input('btn-conc-estoque', 'n_clicks'),
     Input('btn-conc-car', 'n_clicks'),
     Input('btn-compras', 'n_clicks'),
     Input('btn-atualiza-entradas-std', 'n_clicks')],
    [State('cmd-year', 'value'),
     State('cmd-month', 'value')]
)
def run_pipeline_step(btn_master, btn1_nfi, btn1_nf, btn2_nf, btn2_nfi, btn2_5, btn3, btn4, btn5, 
                      btn_conc_estoque, btn_conc_car, btn_compras, btn_atualiza_std, year, month):
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
        'btn-step5': 'step5_report',
        'btn-conc-estoque': 'conc_estoque',
        'btn-conc-car': 'conc_car',
        'btn-compras': 'compras',
        'btn-atualiza-entradas-std': 'step3_update_entradas' # Reusing the step name as it maps to the same script
    }

    if not year or not month:
        return "Error: Year and Month must be specified."

    cmd = ["python", "master_pipeline.py", "--year", str(year), "--month", str(month)]

    if button_id == 'btn-master':
        # Run all steps (no --step argument)
        # We might want to pass --force if the user wants to force run everything?
        # For now, let's just run standard pipeline which checks dependencies.
        # User can use force if they want, but we don't have a checkbox for it yet.
        # Let's assume standard run.
        pass 
    else:
        step_name = step_map.get(button_id)
        if not step_name:
            return "Unknown button."
        
        cmd.extend(["--step", step_name])
        cmd.append("--force") # Force individual steps as per previous behavior

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
