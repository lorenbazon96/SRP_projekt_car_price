from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def transform_condition_dim(car_df, csv_df=None):
    mysql_cond = car_df.select(col("condition").cast("float").alias("condition_val"))

    if csv_df is not None and "condition" in csv_df.columns:
        csv_cond = csv_df.select(col("condition").cast("float").alias("condition_val"))
        combined = mysql_cond.unionByName(csv_cond)
    else:
        combined = mysql_cond

    distinct_cond = (
        combined
        .filter(col("condition_val").isNotNull())
        .dropDuplicates(["condition_val"])
    )

    w = Window.orderBy("condition_val")
    final_df = (
        distinct_cond
        .withColumn("condition_tk", row_number().over(w))
        .select("condition_tk", "condition_val")
    )
    return final_df
