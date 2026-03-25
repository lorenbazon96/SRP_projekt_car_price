import pandas as pd
import numpy as np

# nazivi izlaznih i ulaznih datoteka
CSV_FILE = "car_prices.csv"
OUTPUT_FILE = "car_prices_processed.csv"
OUTPUT_FILE_80 = "car_prices_processed_80.csv"
OUTPUT_FILE_20 = "car_prices_processed_20.csv"

df = pd.read_csv(CSV_FILE)
print("Original size:", df.shape)


# standardizacija naziva stupaca
df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace(' ', '_')

# čišćenje string vrijednosti
clean_cols = [
    'make', 'model', 'trim', 'body',
    'color', 'transmission', 'state',
    'seller', 'interior', 'saledate'
]

# Za svaki od navedenih stupaca:
# - pretvaramo vrijednosti u string
# - uklanjamo praznine na početku i kraju
# - pretvaramo tekst u mala slova
for col in clean_cols:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.lower()

# Vrijednosti koje tretiramo kao lažno nedostajuće podatke
# npr. "-", "unknown", "none", "n/a" i slično
fake = [
    '', '—', '-', '--', 'nan', 'none', 'null',
    'n/a', 'na', 'unknown', 'unspecified',
    'not specified', 'other', '?', 'no color'
]

# U stupcima trim, body i color takve vrijednosti zamjenjujemo s pd.NA
# kako bismo ih kasnije mogli lako ukloniti
for col in ['trim', 'body', 'color']:
    if col in df.columns:
        df[col] = df[col].replace(fake, pd.NA)

# U svim ostalim tekstualnim stupcima lažno nedostajuće vrijednosti
# mijenjamo u np.nan
for col in clean_cols:
    if col in df.columns:
        df[col] = df[col].replace(fake, np.nan)

# pretvaranje u numeričke tipove
# Vrijednosti u ovim stupcima pretvaramo u brojeve
# Ako pretvorba nije moguća, postavlja se NaN
df['year'] = pd.to_numeric(df['year'], errors='coerce')
df['sellingprice'] = pd.to_numeric(df['sellingprice'], errors='coerce')
df['mmr'] = pd.to_numeric(df['mmr'], errors='coerce')
df['odometer'] = pd.to_numeric(df['odometer'], errors='coerce')
df['condition'] = pd.to_numeric(df['condition'], errors='coerce')

# uklanjanje redaka s nedostajućim ključnim podacima
df = df.dropna(subset=[
    'make', 'model', 'year', 'sellingprice', 'state', 'interior'
])

# Brišemo redove gdje nedostaje trim, body ili color
df = df.dropna(subset=['trim', 'body', 'color'])

# Mapa skraćenica saveznih država/provincija u puni naziv
state_map = {
    'ab': 'alberta',
    'al': 'alabama',
    'az': 'arizona',
    'ca': 'california',
    'co': 'colorado',
    'fl': 'florida',
    'ga': 'georgia',
    'hi': 'hawaii',
    'il': 'illinois',
    'in': 'indiana',
    'la': 'louisiana',
    'ma': 'massachusetts',
    'md': 'maryland',
    'mi': 'michigan',
    'mn': 'minnesota',
    'mo': 'missouri',
    'ms': 'mississippi',
    'nc': 'north carolina',
    'ne': 'nebraska',
    'nj': 'new jersey',
    'nm': 'new mexico',
    'ns': 'nova scotia',
    'nv': 'nevada',
    'ny': 'new york',
    'oh': 'ohio',
    'ok': 'oklahoma',
    'on': 'ontario',
    'or': 'oregon',
    'pa': 'pennsylvania',
    'pr': 'puerto rico',
    'qc': 'quebec',
    'sc': 'south carolina',
    'tn': 'tennessee',
    'tx': 'texas',
    'ut': 'utah',
    'va': 'virginia',
    'wa': 'washington',
    'wi': 'wisconsin'
}

# Skup svih valjanih punih naziva država
valid_full_states = set(state_map.values())

# Funkcija za normalizaciju vrijednosti u stupcu state
def normalize_state(value):
    if pd.isna(value):
        return np.nan
    # Čistimo vrijednost: pretvaramo u string, mičemo razmake, mala slova
    value = str(value).strip().lower()

    # Ako je unesena skraćenica, pretvaramo je u puni naziv
    if value in state_map:
        return state_map[value]

    # Ako je već puni naziv i nalazi se među valjanima, ostavljamo ga
    if value in valid_full_states:
        return value

    # sve ostalo je nevaljano
    return np.nan

# Primjena funkcije na cijeli stupac state
df['state'] = df['state'].apply(normalize_state)

# Brišemo redove gdje state nakon čišćenja nije valjan
df = df.dropna(subset=['state'])

# Pretvaramo nazive država u oblik s velikim početnim slovima radi ljepšeg prikaza
df['state'] = df['state'].str.title()

# Zadržavamo samo automobile od 1980. godine nadalje
df = df[df['year'] >= 1980]

# Zadržavamo samo redove gdje je prodajna cijena veća od 0
df = df[df['sellingprice'] > 0]

# Condition mora biti unutar realnog raspona 1-50
df = df[(df['condition'] >= 1) & (df['condition'] <= 50)]

# Kilometraža ne smije biti negativna
df = df[df['odometer'] >= 0]

# MMR mora biti veći od 0
df = df[df['mmr'] > 0]

# Zadržavamo samo redove gdje je transmission automatic ili manual
df = df[df['transmission'].isin(['automatic', 'manual'])]

# Ispis veličine skupa nakon svih koraka čišćenja
print("After preprocessing:", df.shape)

# Nasumično uzimamo 20% podataka za jedan podskup
df20 = df.sample(frac=0.2, random_state=1)

# Ostatak podataka čini 80% podskup
df80 = df.drop(df20.index)

# -----------------------------
# SPREMANJE
# -----------------------------
# Spremamo cijeli obrađeni skup
df.to_csv(OUTPUT_FILE, index=False)

# Spremamo 80% podataka
df80.to_csv(OUTPUT_FILE_80, index=False)

# Spremamo 20% podataka
df20.to_csv(OUTPUT_FILE_20, index=False)

print("Saved files:")
print("-", OUTPUT_FILE)
print("-", OUTPUT_FILE_80)
print("-", OUTPUT_FILE_20)

# Ispis prvih nekoliko redaka obrađenog skupa podataka
print("\nPreview:")
print(df.head())