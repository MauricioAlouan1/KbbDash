import gc
import pandas as pd
from pathlib import Path

# ⚙️ Configurações
ANO = 2025
MES = 11
COBERTURA_MINIMA = 30

# 📂 Caminhos possíveis
paths_to_try = [
    Path(f"/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/clean/{ANO}_{MES:02}"),
    Path(f"/home/simon/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/clean/{ANO}_{MES:02}")
]
base_path = next((p for p in paths_to_try if p.exists()), None)
if not base_path:
    raise FileNotFoundError("❌ Nenhum dos caminhos encontrados.")

# 📄 Arquivos
arquivo_resumo = base_path / f"R_ResumoU6M_{ANO}_{MES:02}.xlsm"
arquivo_estoque = base_path / f"R_Estoq_fdm_{ANO}_{MES:02}.xlsx"

# 📊 Leitura dos dados
o_nfci = pd.read_excel(arquivo_resumo, sheet_name="O_NFCI")
l_lpi = pd.read_excel(arquivo_resumo, sheet_name="L_LPI")

# 📦 Leitura da planilha de estoque correta (segunda aba: "PT01")
estoque = pd.read_excel(arquivo_estoque, sheet_name="PT01")

# 🧼 Padroniza nomes
for df in [o_nfci, l_lpi, estoque]:
    df.columns = df.columns.str.upper()

# Renomeia colunas do estoque
estoque = estoque.rename(columns={
    "CODIGO_INV": "CODPF",
    "TOTAL": "ESTQ_ATUAL"
})

# 🛒 Vendas O_NFCI
vendas_nfci = (
    o_nfci.groupby(["CODPF", "ANOMES"])["QT"]
    .sum()
    .reset_index()
    .rename(columns={"QT": "VENDAS_NFCI"})
)

# 🛒 Vendas L_LPI (KAB == 1)
l_lpi_filtered = l_lpi[l_lpi["KAB"] == 1]
vendas_lpi = (
    l_lpi_filtered.groupby(["CODPF", "ANOMES"])["QT"]
    .sum()
    .reset_index()
    .rename(columns={"QT": "VENDAS_LPI"})
)

# 🔄 Combina as vendas
vendas = pd.merge(vendas_nfci, vendas_lpi, on=["CODPF", "ANOMES"], how="outer").fillna(0)
vendas["VENDAS_TOTAIS"] = vendas["VENDAS_NFCI"] + vendas["VENDAS_LPI"]

# 📦 Junta com estoque
relatorio = pd.merge(vendas, estoque[["CODPF", "ESTQ_ATUAL"]], on="CODPF", how="left").fillna(0)

# 📈 Calcula necessidade de compra
relatorio["NECESSIDADE_COMPRA"] = (COBERTURA_MINIMA - relatorio["ESTQ_ATUAL"]).clip(lower=0)

# ✅ Resultado final
# ✅ Pivota vendas por mês (uma coluna por mês com total vendido)
vendas_pivot = relatorio.pivot_table(
    index="CODPF",
    columns="ANOMES",
    values="VENDAS_TOTAIS",
    aggfunc="sum",
    fill_value=0
)

# ✅ Pivota vendas por mês (uma coluna por mês com total vendido)
vendas_pivot.columns = [f"V_{col}" for col in vendas_pivot.columns]

# 🔗 Junta com estoque atual
relatorio_final = vendas_pivot.merge(estoque[["CODPF", "ESTQ_ATUAL"]], on="CODPF", how="left").fillna(0)

# 📉 Calcula média mensal e meses de estoque
colunas_vendas = [col for col in relatorio_final.columns if col.startswith("V_")]
relatorio_final["VENDAS_MEDIA"] = relatorio_final[colunas_vendas].mean(axis=1)
relatorio_final["MESES_ESTOQUE"] = (relatorio_final["ESTQ_ATUAL"] / relatorio_final["VENDAS_MEDIA"]).round(1)

# Ordena por menor cobertura
relatorio_final = relatorio_final.sort_values("MESES_ESTOQUE")

# 📤 Salva em Excel
output_path = base_path / f"Relatorio_Compras_Pivot_{ANO}_{MES:02}.xlsx"
relatorio_final.to_excel(output_path, index=False)
print(f"✅ Relatório final salvo em: {output_path}")

# 📤 Exporta (opcional)
output_path = base_path / f"R_Compras_{ANO}_{MES:02}.xlsx"
relatorio.to_excel(output_path, index=False)

print(f"✅ Relatório salvo em: {output_path}")

del o_nfci, l_lpi, estoque, vendas_nfci, vendas_lpi, vendas, relatorio
gc.collect()