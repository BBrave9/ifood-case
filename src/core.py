# core.py: Read & Write generic functions --------------------------------------------------------------------
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.utils import AnalysisException
from config import *
from transforms import (
    transform_yellow_taxi,
    transform_green_taxi,
    transform_fhv_taxi,
    transform_hv_fhv_taxi,
)

# -----------------------------------------------------------------
# 1. process_taxi_data function
# -----------------------------------------------------------------
def process_taxi_data(trip_type_folder: str, year: int, month: int):
    """
    Function to read BRONZE data from S3, apply transformations and write to Delta.
    Partition by year and month.
    """

    trip_cfg = TRIP_CONFIG[trip_type_folder]
    silver_suffix = trip_cfg["silver_suffix"]

    bronze_path = (f"{BRONZE_S3_BASE_PATH}{trip_type_folder}/parquet/year={year}/month={month:02d}")
    silver_path = f"{SILVER_S3_BASE_PATH}{silver_suffix}"

    print(f"\n--- {trip_type_folder}  {year}/{month:02d} ---")
    print(f"BRONZE: {bronze_path}")
    print(f"SILVER: {silver_path}")

    # Read bronze data
    try:
        df_bronze: DataFrame = spark.read.option("mergeSchema", "true").parquet(bronze_path)
    except AnalysisException as e:
        print(f"WARN!!! Dados não encontrados em {bronze_path}: {e}")
        return

    # call transform functions according to trip type
    if trip_type_folder == "yellow_taxi_trips":
        df_silver = transform_yellow_taxi(df_bronze)
    elif trip_type_folder == "green_taxi_trips":
        df_silver = transform_green_taxi(df_bronze)
    elif trip_type_folder == "for_hire_vehicle_trips":
        df_silver = transform_fhv_taxi(df_bronze)
    elif trip_type_folder == "high_volume_for_hire_vehicle_trips":
        df_silver = transform_hv_fhv_taxi(df_bronze)
    else:
        print(f"ERRO!! Tipo de viagem não suportado: {trip_type_folder}")
        return

    # Add metadata columns
    df_silver = (
        df_silver
          .withColumn("year",  lit(year))
          .withColumn("month", lit(month))
          .withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("trip_type", lit(trip_type_folder.replace("_trips", "")))
    )

    # Write to Delta
    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("replaceWhere", f"year = {year} AND month = {month}")
        .partitionBy("year", "month")
        .save(silver_path)
    )

    print("Concluído com sucesso!")
