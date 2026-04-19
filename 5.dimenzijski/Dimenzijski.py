from sqlalchemy import create_engine, text
import pandas as pd

# ================= KONEKCIJA =================
engine = create_engine("mysql+pymysql://root:root@localhost:3306/cars_price")

# ================= UČITAVANJE CSV =================
print("UČITAVAM CSV...")
df = pd.read_csv("car_prices_processed_80.csv")

print("Original rows:", len(df))
print(df.columns)

# ================= DATUM =================

df['saledate'] = df['saledate'].astype(str).str.replace(r'\s*gmt.*$', '', regex=True, case=False)
df['saledate'] = pd.to_datetime(df['saledate'], format='mixed', errors='coerce')

# ================= CLEAN =================
df = df.dropna(subset=[
    'make','model','state','seller',
    'sellingprice','mmr','odometer','saledate'
])

print("Rows after cleaning:", len(df))
print("Unique years in saledate:", sorted(df['saledate'].dt.year.unique()))

# ================= STAGING =================
df.to_sql('stg_car_prices', engine, if_exists='replace', index=False)

# ================= DROP =================
print("BRIŠEM TABLICE...")

with engine.begin() as conn:
    conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
    conn.execute(text("DROP TABLE IF EXISTS fact_vehicle_sales"))
    conn.execute(text("DROP TABLE IF EXISTS dim_vozilo"))
    conn.execute(text("DROP TABLE IF EXISTS dim_datum"))
    conn.execute(text("DROP TABLE IF EXISTS dim_lokacija"))
    conn.execute(text("DROP TABLE IF EXISTS dim_prodavac"))
    conn.execute(text("DROP TABLE IF EXISTS dim_stanje"))
    conn.execute(text("DROP TABLE IF EXISTS stg_car_prices"))
    conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))


df.to_sql('stg_car_prices', engine, if_exists='replace', index=False)

# ================= CREATE =================
print("KREIRAM TABLICE...")

with engine.begin() as conn:

    # SCD Type 2 na dim_vozilo
    conn.execute(text("""
    CREATE TABLE dim_vozilo (
        vehicle_tk INT AUTO_INCREMENT PRIMARY KEY,
        make VARCHAR(50),
        model VARCHAR(50),
        trim VARCHAR(50),
        body VARCHAR(50),
        transmission VARCHAR(50),
        color VARCHAR(50),
        interior VARCHAR(50),
        year INT,
        valid_from DATE NOT NULL DEFAULT (CURRENT_DATE),
        valid_to DATE DEFAULT '9999-12-31',
        is_current TINYINT NOT NULL DEFAULT 1
    )
    """))

    conn.execute(text("""
    CREATE TABLE dim_datum (
        date_tk INT AUTO_INCREMENT PRIMARY KEY,
        full_date DATE,
        year INT,
        month INT,
        day INT,
        quarter INT,
        day_of_week INT
    )
    """))

    conn.execute(text("""
    CREATE TABLE dim_lokacija (
        location_tk INT AUTO_INCREMENT PRIMARY KEY,
        state VARCHAR(50)
    )
    """))

    conn.execute(text("""
    CREATE TABLE dim_prodavac (
        seller_tk INT AUTO_INCREMENT PRIMARY KEY,
        seller VARCHAR(100)
    )
    """))

    conn.execute(text("""
    CREATE TABLE dim_stanje (
        condition_tk INT AUTO_INCREMENT PRIMARY KEY,
        condition_val FLOAT
    )
    """))

    conn.execute(text("""
    CREATE TABLE fact_vehicle_sales (
        fact_tk INT AUTO_INCREMENT PRIMARY KEY,
        vehicle_tk INT,
        date_tk INT,
        location_tk INT,
        seller_tk INT,
        condition_tk INT,
        sale_id VARCHAR(50),
        selling_price FLOAT,
        mmr FLOAT,
        odometer FLOAT,
        FOREIGN KEY (vehicle_tk) REFERENCES dim_vozilo(vehicle_tk),
        FOREIGN KEY (date_tk) REFERENCES dim_datum(date_tk),
        FOREIGN KEY (location_tk) REFERENCES dim_lokacija(location_tk),
        FOREIGN KEY (seller_tk) REFERENCES dim_prodavac(seller_tk),
        FOREIGN KEY (condition_tk) REFERENCES dim_stanje(condition_tk)
    )
    """))

# ================= DIM INSERT =================
print("PUNIM DIMENZIJE...")

with engine.begin() as conn:

    conn.execute(text("""
    INSERT INTO dim_vozilo 
    (make, model, trim, body, transmission, color, interior, year, valid_from, valid_to, is_current)
    SELECT DISTINCT 
        make, model, trim, body, transmission, color, interior, year,
        CURDATE(), '9999-12-31', 1
    FROM stg_car_prices
    """))

    conn.execute(text("""
    INSERT INTO dim_lokacija (state)
    SELECT DISTINCT state FROM stg_car_prices
    """))

    conn.execute(text("""
    INSERT INTO dim_prodavac (seller)
    SELECT DISTINCT seller FROM stg_car_prices
    """))

    conn.execute(text("""
    INSERT INTO dim_stanje (condition_val)
    SELECT DISTINCT `condition` FROM stg_car_prices
    """))

    conn.execute(text("""
    INSERT INTO dim_datum (full_date, year, month, day, quarter, day_of_week)
    SELECT DISTINCT
        DATE(saledate),
        YEAR(saledate),
        MONTH(saledate),
        DAY(saledate),
        QUARTER(saledate),
        DAYOFWEEK(saledate)
    FROM stg_car_prices
    WHERE saledate IS NOT NULL
    """))

# ================= FACT =================
print("PUNIM FACT...")

with engine.begin() as conn:

    conn.execute(text("""
    INSERT INTO fact_vehicle_sales (
        vehicle_tk,
        date_tk,
        location_tk,
        seller_tk,
        condition_tk,
        sale_id,
        selling_price,
        mmr,
        odometer
    )
    SELECT
        v.vehicle_tk,
        d.date_tk,
        l.location_tk,
        p.seller_tk,
        s.condition_tk,
        stg.vin,
        stg.sellingprice,
        stg.mmr,
        stg.odometer
    FROM stg_car_prices stg
    JOIN dim_vozilo v 
        ON stg.make = v.make 
        AND stg.model = v.model
        AND COALESCE(stg.trim,'') = COALESCE(v.trim,'')
        AND COALESCE(stg.body,'') = COALESCE(v.body,'')
        AND COALESCE(stg.transmission,'') = COALESCE(v.transmission,'')
        AND COALESCE(stg.color,'') = COALESCE(v.color,'')
        AND COALESCE(stg.interior,'') = COALESCE(v.interior,'')
        AND stg.year = v.year
        AND v.is_current = 1
    JOIN dim_datum d 
        ON DATE(stg.saledate) = d.full_date
    JOIN dim_lokacija l 
        ON stg.state = l.state
    JOIN dim_prodavac p 
        ON stg.seller = p.seller
    JOIN dim_stanje s 
        ON stg.`condition` = s.condition_val
    """))

# ================= PROVJERA =================
print("PROVJERA...")

tables = [
    "stg_car_prices",
    "dim_vozilo",
    "dim_datum",
    "dim_lokacija",
    "dim_prodavac",
    "dim_stanje",
    "fact_vehicle_sales"
]


print("GOTOVO ✅")
