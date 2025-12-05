# Dash_salesmargin.py
from Dash_shared import app, load_data
from dash import html, dcc, Input, Output
import pandas as pd
import plotly.graph_objs as go

# One-time load (you can refresh in the callback if your data changes frequently)
# One-time load (you can refresh in the callback if your data changes frequently)
data_raw = load_data()

# Extract the DataFrame we need (Conc_Estoq usually has VV_tot, Mrg_tot)
df_raw = None
if isinstance(data_raw, dict):
    # Try specific keys
    for key in ["Conc_Estoq - Conc", "Conc_Estoq - Child", "Conc_Estoq"]:
        if key in data_raw:
            df_raw = data_raw[key]
            break
    # Fallback: look for any sheet with VV_tot
    if df_raw is None:
        for key, df in data_raw.items():
            if "VV_tot" in df.columns:
                df_raw = df
                break
else:
    df_raw = data_raw

salesmargin_layout = html.Div(
    [
        html.H3("Vendas & Margem (Consolidado)"),
        dcc.Graph(id="sales-margin-graph"),
        html.Div(
            "Usa os filtros globais (topo): período, empresa e marketplace.",
            style={"fontSize": 12, "color": "#666", "marginTop": "8px"},
        ),
    ],
    style={"padding": "12px"},
)


@app.callback(
    Output("sales-margin-graph", "figure"),
    [
        Input("date-picker", "start_date"),
        Input("date-picker", "end_date"),
        Input("company-filter", "value"),
        Input("marketplace-filter", "value"),
    ],
)
def update_sales_margin(start_date, end_date, empresas, marketplaces):
    if df_raw is None or df_raw.empty:
        return go.Figure(layout={"annotations": [dict(text="Sem dados", showarrow=False)]})

    df = df_raw.copy()

    # Date filter
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
        if start_date:
            df = df[df["DATE"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["DATE"] <= pd.to_datetime(end_date)]

    # Company filter
    if empresas:
        df = df[df["EMPRESA"].isin(empresas)]

    # Marketplace filter (IMPORTANT: your column is MP, not MARKETPLACE)
    if marketplaces:
        col_mp = "MP" if "MP" in df.columns else "MARKETPLACE"
        df = df[df[col_mp].isin(marketplaces)]

    # Aggregate by day
    # Adjust column names below if your metrics are different
    val_cols = []
    if "VV_tot" in df.columns:
        val_cols.append("VV_tot")
    if "Mrg_tot" in df.columns:
        val_cols.append("Mrg_tot")

    if not val_cols:
        # fallback to any likely columns (customize as needed)
        for c in ["VV_2b", "VV_2c", "Mrg_2b", "Mrg_2c"]:
            if c in df.columns:
                val_cols.append(c)

    if not val_cols:
        return go.Figure(layout={"annotations": [dict(text="Sem colunas de valor/margem", showarrow=False)]})

    g = df.groupby("DATE", as_index=False)[val_cols].sum()

    fig = go.Figure()
    for c in val_cols:
        fig.add_trace(go.Scatter(x=g["DATE"], y=g[c], mode="lines+markers", name=c))

    fig.update_layout(
        margin=dict(l=30, r=10, t=30, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_title="Data",
        yaxis_title="Valor (R$)",
    )
    return fig
