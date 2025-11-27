import pandas as pd

df = pd.read_csv("RTN_MASTER_preview.csv", dtype=str)
print("\n=== Primeras 15 fechas detectadas en el CSV ===\n")
print(df["date"].head(15).to_list())
