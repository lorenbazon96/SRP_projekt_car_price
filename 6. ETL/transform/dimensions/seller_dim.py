from pyspark.sql.functions import col, trim, lower, when, lit, row_number
from pyspark.sql.window import Window


def _norm(c):
    return when(col(c).isNull(), lit(None)).otherwise(lower(trim(col(c).cast("string"))))


def transform_seller_dim(seller_df, csv_df=None):
    mysql_sellers = seller_df.select(_norm("seller").alias("seller"))

    if csv_df is not None and "seller" in csv_df.columns:
        csv_sellers = csv_df.select(_norm("seller").alias("seller"))
        combined = mysql_sellers.unionByName(csv_sellers)
    else:
        combined = mysql_sellers

    distinct_sellers = (
        combined
        .filter(col("seller").isNotNull() & (col("seller") != ""))
        .dropDuplicates(["seller"])
    )

    w = Window.orderBy("seller")
    final_df = (
        distinct_sellers
        .withColumn("seller_tk", row_number().over(w))
        .select("seller_tk", "seller")
    )
    return final_df
