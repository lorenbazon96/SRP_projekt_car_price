import pymysql
from pyspark.sql import DataFrame
from db_config import TARGET_DW, jdbc_url, jdbc_props


def _connect_no_db():
    return pymysql.connect(
        host=TARGET_DW["host"],
        port=TARGET_DW["port"],
        user=TARGET_DW["user"],
        password=TARGET_DW["password"],
        autocommit=True,
    )


def _connect_dw():
    return pymysql.connect(
        host=TARGET_DW["host"],
        port=TARGET_DW["port"],
        user=TARGET_DW["user"],
        password=TARGET_DW["password"],
        database=TARGET_DW["database"],
        autocommit=True,
    )


DDL = {
    "dim_vozilo": """
        CREATE TABLE dim_vozilo (
            vehicle_tk    INT PRIMARY KEY,
            make          VARCHAR(100),
            model         VARCHAR(150),
            trim          VARCHAR(150),
            body          VARCHAR(100),
            transmission  VARCHAR(50),
            color         VARCHAR(50),
            interior      VARCHAR(50),
            year          INT,
            version       INT NOT NULL DEFAULT 1,
            valid_from    DATE NOT NULL,
            valid_to      DATE NOT NULL DEFAULT '9999-12-31',
            is_current    TINYINT NOT NULL DEFAULT 1
        )
    """,
    "dim_datum": """
        CREATE TABLE dim_datum (
            date_tk      INT PRIMARY KEY,
            full_date    DATE NOT NULL,
            year         INT,
            month        INT,
            day          INT,
            quarter      INT,
            day_of_week  INT,
            UNIQUE KEY uq_dim_datum_full_date (full_date)
        )
    """,
    "dim_lokacija": """
        CREATE TABLE dim_lokacija (
            location_tk  INT PRIMARY KEY,
            state        VARCHAR(100),
            UNIQUE KEY uq_dim_lokacija_state (state)
        )
    """,
    "dim_prodavac": """
        CREATE TABLE dim_prodavac (
            seller_tk INT PRIMARY KEY,
            seller    VARCHAR(255),
            UNIQUE KEY uq_dim_prodavac_seller (seller)
        )
    """,
    "dim_stanje": """
        CREATE TABLE dim_stanje (
            condition_tk  INT PRIMARY KEY,
            condition_val FLOAT,
            UNIQUE KEY uq_dim_stanje_val (condition_val)
        )
    """,
    "fact_vehicle_sales": """
        CREATE TABLE fact_vehicle_sales (
            fact_tk        BIGINT PRIMARY KEY,
            vehicle_tk     INT,
            date_tk        INT,
            location_tk    INT,
            seller_tk      INT,
            condition_tk   INT,
            sale_id        VARCHAR(50),
            selling_price  FLOAT,
            mmr            FLOAT,
            odometer       FLOAT,
            FOREIGN KEY (vehicle_tk)   REFERENCES dim_vozilo(vehicle_tk),
            FOREIGN KEY (date_tk)      REFERENCES dim_datum(date_tk),
            FOREIGN KEY (location_tk)  REFERENCES dim_lokacija(location_tk),
            FOREIGN KEY (seller_tk)    REFERENCES dim_prodavac(seller_tk),
            FOREIGN KEY (condition_tk) REFERENCES dim_stanje(condition_tk)
        )
    """,
}

DROP_ORDER = [
    "fact_vehicle_sales",
    "dim_vozilo",
    "dim_datum",
    "dim_lokacija",
    "dim_prodavac",
    "dim_stanje",
]

CREATE_ORDER = [
    "dim_vozilo",
    "dim_datum",
    "dim_lokacija",
    "dim_prodavac",
    "dim_stanje",
    "fact_vehicle_sales",
]


def prepare_dw_schema():
    """Kreira (ili re-kreira) sve tablice u ciljnom DW-u."""
    # 1) Stvori bazu ako ne postoji
    conn = _connect_no_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{TARGET_DW['database']}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
    finally:
        conn.close()

    # 2) Drop + Create tablica
    conn = _connect_dw()
    try:
        with conn.cursor() as cur:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            for t in DROP_ORDER:
                cur.execute(f"DROP TABLE IF EXISTS `{t}`")
            for t in CREATE_ORDER:
                cur.execute(DDL[t])
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
    finally:
        conn.close()


def write_spark_df_to_mysql(spark_df: DataFrame, table_name: str, mode: str = "append"):
    print(f"🚀 Pišem u tablicu `{table_name}` (mode `{mode}`)...")
    try:
        (
            spark_df.write
            .jdbc(
                url=jdbc_url(TARGET_DW),
                table=table_name,
                mode=mode,
                properties=jdbc_props(TARGET_DW),
            )
        )
        print(f"✅ Uspješno upisano u `{table_name}`.")
    except Exception as e:
        print(f"❌ Greška pri pisanju u `{table_name}`: {e}")
        raise
