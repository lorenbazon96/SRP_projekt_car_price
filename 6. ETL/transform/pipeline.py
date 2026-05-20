from transform.dimensions.vehicle_dim import transform_vehicle_dim
from transform.dimensions.date_dim import transform_date_dim
from transform.dimensions.location_dim import transform_location_dim
from transform.dimensions.seller_dim import transform_seller_dim
from transform.dimensions.condition_dim import transform_condition_dim
from transform.facts.vehicle_sales_fact import transform_vehicle_sales_fact


def run_transformations(raw_data):
    csv_df = raw_data.get("csv_cars")

    vehicle_dim = transform_vehicle_dim(
        car_df=raw_data["car"],
        model_df=raw_data["model"],
        make_df=raw_data["make"],
        csv_df=csv_df,
    ).cache()
    print("1️⃣  dim_vozilo pripremljen")

    date_dim = transform_date_dim(
        selling_df=raw_data["selling"],
        csv_df=csv_df,
    ).cache()
    print("2️⃣  dim_datum pripremljen")

    location_dim = transform_location_dim(
        selling_df=raw_data["selling"],
        csv_df=csv_df,
    ).cache()
    print("3️⃣  dim_lokacija pripremljen")

    seller_dim = transform_seller_dim(
        seller_df=raw_data["seller"],
        csv_df=csv_df,
    ).cache()
    print("4️⃣  dim_prodavac pripremljen")

    condition_dim = transform_condition_dim(
        car_df=raw_data["car"],
        csv_df=csv_df,
    ).cache()
    print("5️⃣  dim_stanje pripremljen")

    fact_sales = transform_vehicle_sales_fact(
        raw_data=raw_data,
        vehicle_dim=vehicle_dim,
        date_dim=date_dim,
        location_dim=location_dim,
        seller_dim=seller_dim,
        condition_dim=condition_dim,
    )
    print("6️⃣  fact_vehicle_sales pripremljen")

    return {
        "dim_vozilo": vehicle_dim,
        "dim_datum": date_dim,
        "dim_lokacija": location_dim,
        "dim_prodavac": seller_dim,
        "dim_stanje": condition_dim,
        "fact_vehicle_sales": fact_sales,
    }
