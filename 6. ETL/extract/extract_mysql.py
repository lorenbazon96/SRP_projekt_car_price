from spark_session import get_spark_session
from db_config import SOURCE_DB, jdbc_url, jdbc_props


def extract_table(table_name):
    spark = get_spark_session()
    return spark.read.jdbc(
        url=jdbc_url(SOURCE_DB),
        table=table_name,
        properties=jdbc_props(SOURCE_DB),
    )


def extract_all_tables():
    """
    Učitava tablice iz cars_price
    """
    return {
        "make": extract_table("make"),
        "model": extract_table("model"),
        "seller": extract_table("seller"),
        "car": extract_table("car"),
        "selling": extract_table("selling"),
    }
