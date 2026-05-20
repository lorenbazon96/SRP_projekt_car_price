import os
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

if not os.environ.get("SPARK_HOME"):
    try:
        import pyspark, ctypes
        _pyspark_dir = os.path.dirname(pyspark.__file__)
        GetShortPathName = ctypes.windll.kernel32.GetShortPathNameW
        buf = ctypes.create_unicode_buffer(260)
        if GetShortPathName(_pyspark_dir, buf, 260):
            os.environ["SPARK_HOME"] = buf.value
        else:
            os.environ["SPARK_HOME"] = _pyspark_dir
    except Exception:
        pass

from extract.extract_mysql import extract_all_tables
from extract.extract_csv import extract_from_csv
from transform.pipeline import run_transformations
from spark_session import get_spark_session
from load.run_loading import write_spark_df_to_mysql, prepare_dw_schema


def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    spark.catalog.clearCache()

    # 1) PRIPREMA CILJNOG SKLADIŠTA (DW)
    print("🛠️  Pripremam DW shemu (drop + create dimenzije i činjenicu)")
    prepare_dw_schema()
    print("✅ DW shema spremna")

    # 2) EKSTRAKCIJA
    print("🚀 Pokrećem ekstrakciju podataka")
    mysql_dfs = extract_all_tables()
    csv_df = extract_from_csv("../2. Predprocesiranje/car_prices_processed_20.csv")
    raw_data = {**mysql_dfs, "csv_cars": csv_df}
    print("✅ Ekstrakcija dovršena")

    # 3) TRANSFORMACIJA
    print("🚀 Pokrećem transformaciju podataka")
    load_ready_dict = run_transformations(raw_data)
    print("✅ Transformacija dovršena")

    # 4) UČITAVANJE U DW
    print("🚀 Pokrećem učitavanje u DW")
    load_order = [
        "dim_vozilo",
        "dim_datum",
        "dim_lokacija",
        "dim_prodavac",
        "dim_stanje",
        "fact_vehicle_sales",
    ]
    for table_name in load_order:
        if table_name in load_ready_dict:
            write_spark_df_to_mysql(load_ready_dict[table_name], table_name, mode="append")
    print("👏 Učitavanje dovršeno")


if __name__ == "__main__":
    main()
