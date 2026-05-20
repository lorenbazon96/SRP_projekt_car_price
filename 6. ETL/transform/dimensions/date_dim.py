from pyspark.sql.functions import (
    col, to_date, regexp_replace, lower, year as f_year, month as f_month,
    dayofmonth, quarter as f_quarter, dayofweek, row_number,
)
from pyspark.sql.window import Window


def _parse_csv_saledate(c):
    """
    CSV format primjer:
        'tue dec 16 2014 12:30:00 gmt-0800 (pst)'
    """
    cleaned = regexp_replace(lower(c), r"\s*gmt.*$", "")
    return to_date(cleaned, "EEE MMM d yyyy HH:mm:ss")


def transform_date_dim(selling_df, csv_df=None):
    mysql_dates = (
        selling_df
        .select(to_date(col("sale_date_dt")).alias("full_date"))
        .filter(col("full_date").isNotNull())
    )

    if csv_df is not None and "saledate" in csv_df.columns:
        csv_dates = (
            csv_df
            .select(_parse_csv_saledate(col("saledate")).alias("full_date"))
            .filter(col("full_date").isNotNull())
        )
        all_dates = mysql_dates.unionByName(csv_dates)
    else:
        all_dates = mysql_dates

    distinct_dates = all_dates.dropDuplicates(["full_date"])

    enriched = (
        distinct_dates
        .withColumn("year", f_year("full_date"))
        .withColumn("month", f_month("full_date"))
        .withColumn("day", dayofmonth("full_date"))
        .withColumn("quarter", f_quarter("full_date"))
        .withColumn("day_of_week", dayofweek("full_date"))
    )

    w = Window.orderBy("full_date")
    final_df = (
        enriched
        .withColumn("date_tk", row_number().over(w))
        .select("date_tk", "full_date", "year", "month", "day", "quarter", "day_of_week")
    )
    return final_df
