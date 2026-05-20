from pyspark.sql.functions import (
    col, lower, trim, lit, row_number, current_date,
    to_date, when,
)
from pyspark.sql.window import Window

VEHICLE_KEYS = ["make", "model", "trim", "body", "transmission", "color", "interior", "year"]


def _norm_str(c):
    """Lowercase + trim, ali zadržava NULL kao NULL."""
    return when(col(c).isNull(), lit(None)).otherwise(lower(trim(col(c).cast("string"))))


def transform_vehicle_dim(car_df, model_df, make_df, csv_df=None):
    """
    Dim vozila kombinira normalizirane OLTP tablice (car JOIN model JOIN make)
    s plosnatim CSV-om. SCD Type 2 metadata: version, valid_from, valid_to, is_current.
    """
    mysql_df = (
        car_df.alias("c")
        .join(model_df.alias("m"), col("c.model_id") == col("m.id"), "left")
        .join(make_df.alias("mk"), col("m.make_id") == col("mk.id"), "left")
        .select(
            col("mk.make").alias("make"),
            col("m.model").alias("model"),
            col("c.trim").alias("trim"),
            col("c.body").alias("body"),
            col("c.transmission").alias("transmission"),
            col("c.color").alias("color"),
            col("c.interior").alias("interior"),
            col("c.year").cast("int").alias("year"),
        )
    )

    if csv_df is not None:
        csv_part = csv_df.select(
            col("make"),
            col("model"),
            col("trim"),
            col("body"),
            col("transmission"),
            col("color"),
            col("interior"),
            col("year").cast("int").alias("year"),
        )
        combined = mysql_df.unionByName(csv_part)
    else:
        combined = mysql_df

    for k in ["make", "model", "trim", "body", "transmission", "color", "interior"]:
        combined = combined.withColumn(k, _norm_str(k))

    combined = (
        combined
        .filter(col("make").isNotNull() & col("model").isNotNull() & col("year").isNotNull())
        .dropDuplicates(VEHICLE_KEYS)
    )

    w = Window.orderBy("make", "model", "year", "trim", "body", "transmission", "color", "interior")
    final_df = (
        combined
        .withColumn("vehicle_tk", row_number().over(w))
        .withColumn("version", lit(1))
        .withColumn("valid_from", current_date())
        .withColumn("valid_to", to_date(lit("9999-12-31")))
        .withColumn("is_current", lit(1).cast("tinyint"))
        .select(
            "vehicle_tk", "make", "model", "trim", "body", "transmission",
            "color", "interior", "year",
            "version", "valid_from", "valid_to", "is_current",
        )
    )

    return final_df
