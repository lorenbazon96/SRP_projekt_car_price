# db_config.py
# Centralizirana konfiguracija pristupa bazama podataka

# Izvorna OLTP baza (kreirana u koraku 4. Uvoz u bazu i testiranje importa)
SOURCE_DB = {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "cars_price",
    "user": "root",
    "password": "root",
}

# Ciljano skladište podataka (Star schema)
TARGET_DW = {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "cars_price_dw",
    "user": "root",
    "password": "root",
}


def jdbc_url(cfg):
    return f"jdbc:mysql://{cfg['host']}:{cfg['port']}/{cfg['database']}?useSSL=false&allowPublicKeyRetrieval=true"


def jdbc_url_no_db(cfg):
    return f"jdbc:mysql://{cfg['host']}:{cfg['port']}/?useSSL=false&allowPublicKeyRetrieval=true"


def jdbc_props(cfg):
    return {
        "user": cfg["user"],
        "password": cfg["password"],
        "driver": "com.mysql.cj.jdbc.Driver",
    }
