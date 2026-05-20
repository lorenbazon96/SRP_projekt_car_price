from pyspark.sql.functions import col, trim, lower, when, lit, row_number
from pyspark.sql.window import Window


def _norm(c):
    return when(col(c).isNull(), lit(None)).otherwise(lower(trim(col(c).cast("string"))))


def transform_location_dim(selling_df, csv_df=None):
    mysql_loc = selling_df.select(_norm("state").alias("state"))

    if csv_df is not None and "state" in csv_df.columns:
        csv_loc = csv_df.select(_norm("state").alias("state"))
        combined = mysql_loc.unionByName(csv_loc)
    else:
        combined = mysql_loc

    distinct_loc = (
        combined
        .filter(col("state").isNotNull() & (col("state") != ""))
        .dropDuplicates(["state"])
    )

    w = Window.orderBy("state")
    final_df = (
        distinct_loc
        .withColumn("location_tk", row_number().over(w))
        .select("location_tk", "state")
    )
    return final_df
