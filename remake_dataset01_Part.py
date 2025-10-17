import os
import pandas as pd
import numpy as np
import unicodedata
from pathlib import Path
from openpyxl import load_workbook

# === 1️⃣ Get paths & date ===
def get_paths():
    year = input("Enter year (default 2025): ") or "2025"
    month = input("Enter month [1-12] (default 9): ") or "9"
    year, month = int(year), int(month)

    base_dir = Path("/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data")
    clean_dir = base_dir / "clean" / f"{year}_{month:02d}"
    tables_dir = base_dir / "Tables"

    print(f"📆 Selected period: {year}_{month:02d}")
    print(f"📂 Base directory: {base_dir}")
    return year, month, base_dir, clean_dir, tables_dir


# === 2️⃣ Load all data ===
def load_all_sources(year, month, clean_dir, tables_dir):
    def load_xlsx(path):
        if not path.exists():
            print(f"⚠️ Missing: {path}")
            return pd.DataFrame()
        return pd.read_excel(path)

    O_NFCI = load_xlsx(clean_dir / f"O_NFCI_{year}_{month:02d}_clean.xlsx")
    L_LPI = load_xlsx(clean_dir / f"L_LPI_{year}_{month:02d}_clean.xlsx")
    B_Estoq = load_xlsx(clean_dir / f"B_Estoq_{year}_{month:02d}_clean.xlsx")

    T_ProdF = load_xlsx(tables_dir / "T_ProdF.xlsx")
    T_Entradas = load_xlsx(tables_dir / "T_Entradas.xlsx")

    print(f"✅ Loaded O_NFCI: {O_NFCI.shape}")
    print(f"✅ Loaded L_LPI: {L_LPI.shape}")
    print(f"✅ Loaded B_Estoq: {B_Estoq.shape}")

    return O_NFCI, L_LPI, B_Estoq, T_ProdF, T_Entradas

def normalize_cols(df):
    df.columns = [
        unicodedata.normalize("NFKD", c)
        .encode("ascii", "ignore")
        .decode("utf-8")
        .upper()
        .strip()
        for c in df.columns
    ]
    return df

# === 3️⃣ Normalize, merge & attach costs ===
def prepare_partial_data(O_NFCI, L_LPI, B_Estoq, T_ProdF, T_Entradas):
    """
    Normalize column names, merge CODPP (Pai/Filho) relationships,
    and attach last known cost (CU_F) from T_Entradas.
    """

    # --- Normalize ---
    O_NFCI = normalize_cols(O_NFCI)
    L_LPI = normalize_cols(L_LPI)
    B_Estoq = normalize_cols(B_Estoq)
    T_ProdF = normalize_cols(T_ProdF)
    T_Entradas = normalize_cols(T_Entradas)

    print("\n🔠 Columns after normalization:")
    print("O_NFCI:", list(O_NFCI.columns))
    print("L_LPI :", list(L_LPI.columns))
    print("B_Estoq:", list(B_Estoq.columns))

    # --- Merge CODPP from T_ProdF ---
    if "CODIGO DO PRODUTO" in O_NFCI.columns:
        O_NFCI = O_NFCI.merge(
            T_ProdF[["CODPF", "CODPP"]],
            left_on="CODIGO DO PRODUTO",
            right_on="CODPF",
            how="left"
        )
        O_NFCI.drop(columns=["CODPF"], inplace=True, errors="ignore")
        print(f"🧩 CODPP merge added to O_NFCI → has CODPP? {'CODPP' in O_NFCI.columns}")
    else:
        print("⚠️ O_NFCI missing column 'CODIGO DO PRODUTO' — skipped CODPP merge.")

    if "SKU" in L_LPI.columns:
        L_LPI = L_LPI.merge(
            T_ProdF[["CODPF", "CODPP"]],
            left_on="SKU",
            right_on="CODPF",
            how="left"
        )
        L_LPI.drop(columns=["CODPF"], inplace=True, errors="ignore")
        print(f"🧩 CODPP merge added to L_LPI → has CODPP? {'CODPP' in L_LPI.columns}")
    else:
        print("⚠️ L_LPI missing column 'SKU' — skipped CODPP merge.")

    # --- Sanity check ---
    if "CODPP" not in O_NFCI.columns:
        raise KeyError("❌ O_NFCI is missing CODPP after merge.")
    if "CODPP" not in L_LPI.columns:
        raise KeyError("❌ L_LPI is missing CODPP after merge.")

    # --- Attach last cost CU_F from T_Entradas ---
    if {"PAI", "CU_F"}.issubset(T_Entradas.columns):
        cost_map = T_Entradas.set_index("PAI")["CU_F"].to_dict()
        O_NFCI["CU_F"] = O_NFCI["CODPP"].map(cost_map)
        L_LPI["CU_F"] = L_LPI["CODPP"].map(cost_map)

        missing_o = O_NFCI["CU_F"].isna().sum()
        missing_l = L_LPI["CU_F"].isna().sum()
        print(f"💡 attach_last_cost: O_NFCI missing {missing_o}, L_LPI missing {missing_l}")
    else:
        print("⚠️ Columns 'PAI' or 'CU_F' missing in T_Entradas — cost attachment skipped.")

    # --- Return cleaned data ---
    return O_NFCI, L_LPI, B_Estoq, T_Entradas

# === 4️⃣ Create summary ===
def create_summary(L_LPI, T_Entradas):
    def find_best(df, options, label):
        for o in options:
            if o in df.columns:
                print(f"🔹 Using column for {label}: {o}")
                return o
        raise KeyError(f"❌ None of {options} found in columns: {df.columns.tolist()}")

    col_codpp = find_best(L_LPI, ["CODPP"], "product code")
    col_qtd = find_best(L_LPI, ["VENDAS", "QTD", "QUANTIDADE", "QTDE"], "quantity")
    col_vlr = find_best(L_LPI, ["PRECO COM DESCONTO", "VALOR VENDA", "VENDA VLR", "VALOR TOTAL"], "sales value")

    resumo = (
        L_LPI.groupby(col_codpp, as_index=False)
        .agg({col_qtd: "sum", col_vlr: "sum"})
        .rename(columns={col_qtd: "Qtd_Vendida", col_vlr: "Vlr_Venda"})
    )

    resumo = resumo.merge(
        T_Entradas[["PAI", "CU_F"]].rename(columns={"PAI": "CODPP"}),
        on="CODPP", how="left"
    )

    resumo["Margem_R$"] = resumo["Vlr_Venda"] - (resumo["Qtd_Vendida"] * resumo["CU_F"])
    resumo["Margem_%"] = np.where(
        resumo["Vlr_Venda"] > 0,
        resumo["Margem_R$"] / resumo["Vlr_Venda"],
        np.nan
    )

    print(f"✅ Created resumo: {resumo.shape}")
    return resumo


# === 5️⃣ Save results ===
def save_to_excel(O_NFCI, L_LPI, B_Estoq, resumo, clean_dir, year, month):
    output_file = clean_dir / f"R_Resumo_Parcial_{year}_{month:02d}.xlsx"
    with pd.ExcelWriter(output_file, engine="openpyxl", mode="w") as writer:
        O_NFCI.to_excel(writer, sheet_name="O_NFCI", index=False)
        L_LPI.to_excel(writer, sheet_name="L_LPI", index=False)
        B_Estoq.to_excel(writer, sheet_name="B_Estoq", index=False)
        resumo.to_excel(writer, sheet_name="Resumo_Margem", index=False)

    print(f"✅ Partial R_Resumo created and filled: {output_file}")
    return output_file


# === MAIN orchestrator ===
def main():
    year, month, base_dir, clean_dir, tables_dir = get_paths()
    O_NFCI, L_LPI, B_Estoq, T_ProdF, T_Entradas = load_all_sources(year, month, clean_dir, tables_dir)
    O_NFCI, L_LPI, B_Estoq, T_Entradas = prepare_partial_data(O_NFCI, L_LPI, B_Estoq, T_ProdF, T_Entradas)
    resumo = create_summary(L_LPI, T_Entradas)
    save_to_excel(O_NFCI, L_LPI, B_Estoq, resumo, clean_dir, year, month)


if __name__ == "__main__":
    main()
