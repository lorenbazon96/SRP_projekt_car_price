from pyspark.sql import SparkSession

def get_spark_session(app_name="ETL_CarPrice"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", "Connectors/mysql-connector-j-9.2.0.jar")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
