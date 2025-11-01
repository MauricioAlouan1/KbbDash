from pathlib import Path
import pandas as pd

# Show all columns and avoid truncation
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 0)
pd.set_option("display.max_colwidth", None)

# Path to your cache folder
cache = Path("/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data/cache")

# Choose which parquet files to inspect
files = ["L_LPI", "O_NFCI", "Conc_Estoque"]

for name in files:
    path = cache / f"{name}.parquet"
    if path.exists():
        df = pd.read_parquet(path)
        print(f"\n📂 {name}: {df.shape[0]} rows × {df.shape[1]} cols")
        print("🧠 Columns:", list(df.columns))
        print("\n🔝 Top 10 rows:")
        print(df.head(10))
        print("\n🔻 Last 10 rows:")
        print(df.tail(10))
        print("\n" + "─" * 120)
    else:
        print(f"⚠️ {path.name} not found.")
