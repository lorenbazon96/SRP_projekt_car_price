# eda_analysis.py
import pandas as pd
import os

# Putanja do CSV datoteke
base_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(base_dir, "data", "car_prices_processed_80.csv")

# 1. Učitati iz csv u dataframe (pandas)
data = pd.read_csv(csv_path)
print("=" * 60)
print("Checkpoint 1 - Zadatak")
print("=" * 60)

# 2. Pregled prvih 5 redaka
print("\n1. Prvih 5 redaka:")
print(data.head())

# 3. Veličina skupa
print("\n2. Veličina skupa:")
print(f"Redovi: {data.shape[0]}, Stupci: {data.shape[1]}")

# 4. Nazivi stupaca
print("\n3. Nazivi stupaca:")
for i, col in enumerate(data.columns, 1):
    print(f"{i}. {col}")
print("\n4. Tipovi podataka:")
print(data.dtypes)

# 5. Broj nedostajućih vrijednosti po stupcu (.isna)
print("\n4. Nedostajuće vrijednosti po stupcu:")
missing = data.isna().sum()
print(missing[missing > 0] if missing.sum() > 0 else "Nema nedostajućih vrijednosti")

# 6. Jedinstvene vrijednosti (.unique())
print("\n5. Jedinstvene vrijednosti:")

# Sve osim brojčanih vrijednosti su tekstualni stupci)
tekstualni_stupci = ['year','make','model','trim','body','transmission','vin','state','condition','odometer','color','interior','seller','mmr','sellingprice','saledate'
]

for col in tekstualni_stupci:
    if col in data.columns:
        unique_vals = data[col].unique()
        print(f"\n{col}: {len(unique_vals)} jedinstvenih")
        print(unique_vals[:5])  # prvih 5 jedinstvenih vrijednosti

# 7. Frekvencije vrijednosti po stupcu
print("\n6. Frekvencije vrijednosti po stupcu:")

for col in tekstualni_stupci:
    if col in data.columns:
        print(f"\n{col}")
        print(data[col].value_counts())

print("\n" + "=" * 60)
print("ANALIZA ZAVRŠENA")
print("=" * 60)