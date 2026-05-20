from pyspark.sql.functions import (
    col, lower, trim, when, lit, to_date,
    regexp_replace, monotonically_increasing_id,
)


def _norm(c):
    return when(col(c).isNull(), lit(None)).otherwise(lower(trim(col(c).cast("string"))))


def _parse_csv_saledate(c):
    cleaned = regexp_replace(lower(c), r"\s*gmt.*$", "")
    return to_date(cleaned, "EEE MMM d yyyy HH:mm:ss")


def transform_vehicle_sales_fact(
    raw_data,
    vehicle_dim,
    date_dim,
    location_dim,
    seller_dim,
    condition_dim,
):
    car_df = raw_data["car"]
    model_df = raw_data["model"]
    make_df = raw_data["make"]
    seller_src_df = raw_data["seller"]
    selling_df = raw_data["selling"]
    csv_df = raw_data.get("csv_cars")

    mysql_sales = (
        selling_df.alias("s")
        .join(car_df.alias("c"), col("s.car_id") == col("c.id"), "left")
        .join(model_df.alias("m"), col("c.model_id") == col("m.id"), "left")
        .join(make_df.alias("mk"), col("m.make_id") == col("mk.id"), "left")
        .join(seller_src_df.alias("sl"), col("s.seller_id") == col("sl.id"), "left")
        .select(
            col("c.vin").alias("sale_id"),
            col("s.sellingprice").cast("float").alias("selling_price"),
            col("c.mmr").cast("float").alias("mmr"),
            col("c.odometer").cast("float").alias("odometer"),
            to_date(col("s.sale_date_dt")).alias("full_date"),
            _norm("sl.seller").alias("seller"),
            _norm("s.state").alias("state"),
            col("c.condition").cast("float").alias("condition_val"),
            _norm("mk.make").alias("make"),
            _norm("m.model").alias("model"),
            _norm("c.trim").alias("trim"),
            _norm("c.body").alias("body"),
            _norm("c.transmission").alias("transmission"),
            _norm("c.color").alias("color"),
            _norm("c.interior").alias("interior"),
            col("c.year").cast("int").alias("year"),
        )
    )

    if csv_df is not None:
        csv_sales = csv_df.select(
            col("vin").alias("sale_id"),
            col("sellingprice").cast("float").alias("selling_price"),
            col("mmr").cast("float").alias("mmr"),
            col("odometer").cast("float").alias("odometer"),
            _parse_csv_saledate(col("saledate")).alias("full_date"),
            _norm("seller").alias("seller"),
            _norm("state").alias("state"),
            col("condition").cast("float").alias("condition_val"),
            _norm("make").alias("make"),
            _norm("model").alias("model"),
            _norm("trim").alias("trim"),
            _norm("body").alias("body"),
            _norm("transmission").alias("transmission"),
            _norm("color").alias("color"),
            _norm("interior").alias("interior"),
            col("year").cast("int").alias("year"),
        )
        all_sales = mysql_sales.unionByName(csv_sales)
    else:
        all_sales = mysql_sales

    all_sales = all_sales.filter(
        col("full_date").isNotNull()
        & col("state").isNotNull()
        & col("seller").isNotNull()
        & col("make").isNotNull()
        & col("model").isNotNull()
        & col("year").isNotNull()
    )

    veh = vehicle_dim.select(
        col("vehicle_tk"),
        col("make").alias("v_make"),
        col("model").alias("v_model"),
        col("trim").alias("v_trim"),
        col("body").alias("v_body"),
        col("transmission").alias("v_transmission"),
        col("color").alias("v_color"),
        col("interior").alias("v_interior"),
        col("year").alias("v_year"),
    )

    dt = date_dim.select(col("date_tk"), col("full_date").alias("d_full_date"))
    loc = location_dim.select(col("location_tk"), col("state").alias("l_state"))
    sel = seller_dim.select(col("seller_tk"), col("seller").alias("p_seller"))
    cond = condition_dim.select(col("condition_tk"), col("condition_val").alias("c_val"))

    fact = (
        all_sales.alias("s")
        .join(
            veh,
            (col("s.make") == col("v_make"))
            & (col("s.model") == col("v_model"))
            & (col("s.year") == col("v_year"))
            & (col("s.trim").eqNullSafe(col("v_trim")))
            & (col("s.body").eqNullSafe(col("v_body")))
            & (col("s.transmission").eqNullSafe(col("v_transmission")))
            & (col("s.color").eqNullSafe(col("v_color")))
            & (col("s.interior").eqNullSafe(col("v_interior"))),
            "left",
        )
        .join(dt, col("s.full_date") == col("d_full_date"), "left")
        .join(loc, col("s.state") == col("l_state"), "left")
        .join(sel, col("s.seller") == col("p_seller"), "left")
        .join(cond, col("s.condition_val").eqNullSafe(col("c_val")), "left")
        .select(
            col("vehicle_tk"),
            col("date_tk"),
            col("location_tk"),
            col("seller_tk"),
            col("condition_tk"),
            col("s.sale_id").alias("sale_id"),
            col("s.selling_price").alias("selling_price"),
            col("s.mmr").alias("mmr"),
            col("s.odometer").alias("odometer"),
        )
        .filter(
            col("vehicle_tk").isNotNull()
            & col("date_tk").isNotNull()
            & col("location_tk").isNotNull()
            & col("seller_tk").isNotNull()
        )
    )

    fact = fact.withColumn("fact_tk", monotonically_increasing_id()).select(
        "fact_tk",
        "vehicle_tk",
        "date_tk",
        "location_tk",
        "seller_tk",
        "condition_tk",
        "sale_id",
        "selling_price",
        "mmr",
        "odometer",
    )

    return fact
