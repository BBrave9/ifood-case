# transforms.py: Specific transformation functions ---------------------------------------------------
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name, to_timestamp, coalesce, col, sum, lit, when
from pyspark.sql.types import *

# -----------------------------------------------------------------
# 1. Transform Yellow Taxi function
# -----------------------------------------------------------------
def transform_yellow_taxi(df: DataFrame) -> DataFrame:
    select_cols = [
        col("VendorID").cast(IntegerType()).alias("vendor_id"),
        col("tpep_pickup_datetime").cast(TimestampType()).alias("pickup_datetime"),
        col("tpep_dropoff_datetime").cast(TimestampType()).alias("dropoff_datetime"),
        col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        col("trip_distance").cast(DoubleType()).alias("trip_distance"),
        col("RatecodeID").cast(IntegerType()).alias("ratecode_id"),
        col("store_and_fwd_flag").cast(StringType()).alias("store_and_fwd_flag"),
        col("PULocationID").cast(IntegerType()).alias("pu_location_id"),
        col("DOLocationID").cast(IntegerType()).alias("do_location_id"),
        col("payment_type").cast(IntegerType()).alias("payment_type"),
        col("fare_amount").cast(DecimalType(10, 2)).alias("fare_amount"),
        col("extra").cast(DecimalType(10, 2)).alias("extra"),
        col("mta_tax").cast(DecimalType(10, 2)).alias("mta_tax"),
        col("tip_amount").cast(DecimalType(10, 2)).alias("tip_amount"),
        col("tolls_amount").cast(DecimalType(10, 2)).alias("tolls_amount"),
        col("improvement_surcharge").cast(DecimalType(10, 2)).alias("improvement_surcharge"),
        col("total_amount").cast(DecimalType(10, 2)).alias("total_amount"),
        col("congestion_surcharge").cast(DecimalType(10, 2)).alias("congestion_surcharge"),
        col("airport_fee").cast(DecimalType(10, 2)).alias("airport_fee"),
    ]

    # Create df
    df = df.select(*select_cols)

    """
    The column 'cbd_congestion_fee' doesnt exists on the source files between jan and may 2023, 
    but is referencered in the 'data_dictionary_trip_records_yellow' file.
    The following code block was added to accomodate the data dictionary scenario.
    """

    # Check if column cbd_congestion_fee exists and add it to the select_cols list
    if "cbd_congestion_fee" in df.columns:
        df = df.withColumn("cbd_congestion_fee", col("cbd_congestion_fee").cast(DecimalType(10, 2)))
        print('Column cbd_congestion_fee exists on the source')
    # If doesnt exist, add it as NULL
    else:
        print("WARNING: Column 'cbd_congestion_fee' not found on the source. Added as NULL.")
        df = df.withColumn("cbd_congestion_fee", lit(None).cast(DecimalType(10, 2)))

    #Data cleasing
    df = df.dropna(subset=["vendor_id", "pickup_datetime", "dropoff_datetime", "total_amount"])
    return df

# -----------------------------------------------------------------
# 2. Transform Green Taxi function
# -----------------------------------------------------------------
def transform_green_taxi(df: DataFrame) -> DataFrame:
    select_cols = [
        col("VendorID").cast(IntegerType()).alias("vendor_id"),
        col("lpep_pickup_datetime").cast(TimestampType()).alias("pickup_datetime"),
        col("lpep_dropoff_datetime").cast(TimestampType()).alias("dropoff_datetime"),
        col("store_and_fwd_flag").cast(StringType()).alias("store_and_fwd_flag"),
        col("RatecodeID").cast(IntegerType()).alias("ratecode_id"),
        col("PULocationID").cast(IntegerType()).alias("pu_location_id"),
        col("DOLocationID").cast(IntegerType()).alias("do_location_id"),
        col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        col("trip_distance").cast(DoubleType()).alias("trip_distance"),
        col("fare_amount").cast(DecimalType(10, 2)).alias("fare_amount"),
        col("extra").cast(DecimalType(10, 2)).alias("extra"),
        col("mta_tax").cast(DecimalType(10, 2)).alias("mta_tax"),
        col("tip_amount").cast(DecimalType(10, 2)).alias("tip_amount"),
        col("tolls_amount").cast(DecimalType(10, 2)).alias("tolls_amount"),
        col("ehail_fee").cast(IntegerType()).alias("ehail_fee"),
        col("improvement_surcharge").cast(DecimalType(10, 2)).alias("improvement_surcharge"),
        col("total_amount").cast(DecimalType(10, 2)).alias("total_amount"),
        col("payment_type").cast(IntegerType()).alias("payment_type"),
        col("trip_type").cast(IntegerType()).alias("trip_type_code"),
        col("congestion_surcharge").cast(DecimalType(10, 2)).alias("congestion_surcharge")
    ]

    # Create df
    df = df.select(*select_cols)

    """
    The column 'cbd_congestion_fee' doesnt exists on the source files between jan and may 2023, 
    but is referencered in the 'data_dictionary_trip_records_yellow' file.
    The following code block was added to accomodate the data dictionary scenario.
    """

    # Check if column cbd_congestion_fee exists and add it to the select_cols list
    if "cbd_congestion_fee" in df.columns:
        df = df.withColumn("cbd_congestion_fee", col("cbd_congestion_fee").cast(DecimalType(10, 2)))
        print('Column cbd_congestion_fee exists on the source')
    # If doesnt exist, add it as NULL
    else:
        print("WARNING: Column 'cbd_congestion_fee' not found on the source. Added as NULL.")
        df = df.withColumn("cbd_congestion_fee", lit(None).cast(DecimalType(10, 2)))

    # Data cleasing
    df = df.dropna(subset=["vendor_id", "pickup_datetime", "dropoff_datetime", "total_amount"])
    return df

# -----------------------------------------------------------------
# 3. Transform FHV function
# -----------------------------------------------------------------
def transform_fhv_taxi(df: DataFrame) -> DataFrame:
    df = df.select(
        col("dispatching_base_num").cast(StringType()).alias("vendor_id"), # Rename to vendor_id
        col("pickup_datetime").cast(TimestampType()).alias("pickup_datetime"),
        col("dropoff_datetime").cast(TimestampType()).alias("dropoff_datetime"),
        lit(None).cast(IntegerType()).alias("passenger_count"), # Addind passenger_count as Null
        lit(None).cast(DecimalType(10, 2)).alias("total_amount"), # Addind total_amount as Null
        col("PUlocationID").cast(IntegerType()).alias("pu_location_id"),
        col("DOlocationID").cast(IntegerType()).alias("do_location_id"),
        when(col("SR_Flag") == 1, True).otherwise(False).cast(BooleanType()).alias("sr_flag"), # Set True and False cond & cast to bool
        col("Affiliated_base_number").cast(StringType()).alias("affiliated_base_number")
    )
    # Data cleasing
    df = df.dropna(subset=["vendor_id", "pickup_datetime", "dropoff_datetime"])
    return df

# -----------------------------------------------------------------
# 3. Transform HFHV function
# -----------------------------------------------------------------
def transform_hv_fhv_taxi(df: DataFrame) -> DataFrame:
    select_cols = [
        col("hvfhs_license_num").cast(StringType()).alias("vendor_id"), # Rename to vendor_id
        col("dispatching_base_num").cast(StringType()).alias("dispatching_base_num"),
        col("originating_base_num").cast(StringType()).alias("originating_base_num"),
        col("request_datetime").cast(TimestampType()).alias("request_datetime"),
        col("on_scene_datetime").cast(TimestampType()).alias("on_scene_datetime"),
        col("pickup_datetime").cast(TimestampType()).alias("pickup_datetime"),
        col("dropoff_datetime").cast(TimestampType()).alias("dropoff_datetime"),
        lit(None).cast(IntegerType()).alias("passenger_count"), # Addind passenger_count as Null
        col("PULocationID").cast(IntegerType()).alias("pu_location_id"),
        col("DOLocationID").cast(IntegerType()).alias("do_location_id"),
        col("trip_miles").cast(DoubleType()).alias("trip_miles"),
        col("trip_time").cast(IntegerType()).alias("trip_time"),
        col("base_passenger_fare").cast(DecimalType(10, 2)).alias("base_passenger_fare"),
        col("tolls").cast(DecimalType(10, 2)).alias("tolls"),
        col("bcf").cast(DecimalType(10, 2)).alias("bcf"),
        col("sales_tax").cast(DecimalType(10, 2)).alias("sales_tax"),
        col("congestion_surcharge").cast(DecimalType(10, 2)).alias("congestion_surcharge"),
        col("airport_fee").cast(DecimalType(10, 2)).alias("airport_fee"),
        col("tips").cast(DecimalType(10, 2)).alias("tips"),
        col("driver_pay").cast(DecimalType(10, 2)).alias("driver_pay"),
        when(col("shared_request_flag") == 'N', True).otherwise(False).cast(BooleanType()).alias("shared_request_flag"), # Set True and False cond & cast to bool 
        when(col("shared_match_flag") == 'N', True).otherwise(False).cast(BooleanType()).alias("shared_match_flag"), # Set True and False cond & cast to bool
        when(col("access_a_ride_flag") == 'N', True).otherwise(False).cast(BooleanType()).alias("access_a_ride_flag"), # Set True and False cond & cast to bool
        when(col("wav_request_flag") == 'N', True).otherwise(False).cast(BooleanType()).alias("wav_request_flag"), # Set True and False cond & cast to bool
        when(col("wav_match_flag") == 'N', True).otherwise(False).cast(BooleanType()).alias("wav_match_flag") # Set True and False cond & cast to bool
	]

    #Create df
    df = df.select(*select_cols)

    """
    The column 'cbd_congestion_fee' doesnt exists on the source files between jan and may 2023, 
    but is referencered in the 'data_dictionary_trip_records_yellow' file.
    The following code block was added to accomodate the data dictionary scenario.
    """

    # Check if column cbd_congestion_fee exists and add it to the select_cols list
    if "cbd_congestion_fee" in df.columns:
        df = df.withColumn("cbd_congestion_fee", col("cbd_congestion_fee").cast(DecimalType(10, 2)))
        print('Column cbd_congestion_fee exists on the source')
    # If doesnt exist, add it as NULL
    else:
        print("WARNING: Column 'cbd_congestion_fee' not found on the source. Added as NULL.")
        df = df.withColumn("cbd_congestion_fee", lit(None).cast(DecimalType(10, 2)))

    # Calculate total_amount and add the column
    df = df.withColumn(
        "total_amount",
        (coalesce(col("base_passenger_fare"), lit(0)) +
            coalesce(col("tolls"), lit(0)) +
            coalesce(col("bcf"), lit(0)) +
            coalesce(col("sales_tax"), lit(0)) +
            coalesce(col("congestion_surcharge"), lit(0)) +
            coalesce(col("airport_fee"), lit(0))).cast(DecimalType(12,2))
    )

    # Data cleasing
    df = df.dropna(subset=["vendor_id", "pickup_datetime", "dropoff_datetime", "total_amount"])	
    return df
