
from spark_session import get_spark_session


def extract_from_csv(file_path):
    """
    Učitava CSV
    """
    spark = get_spark_session()
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(file_path)
    )
    return df
