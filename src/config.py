# config.py: Initial Setup and Constants Definition ---------------------------------------------------
from pyspark.sql import SparkSession

# SparkSession:
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

# S3 Path
BRONZE_S3_BASE_PATH = "s3://ifood-case-nyc-taxi-data-lake-raw/nyctlc/"
SILVER_S3_BASE_PATH = "s3://ifood-case-nyc-taxi-data-lake-silver/nyc-taxi-trips/"

# Dates range
YEAR    = 2023
MONTHS  = range(1, 6)  # jan-mai

# Set database name
DATABASE_NAME = "`workspace_ifood-case`.nyc_taxi"

# Config source file name <-> silver table name
TRIP_CONFIG = {
    "yellow_taxi_trips": {"silver_suffix": "yellow_silver"},
    "green_taxi_trips": {"silver_suffix": "green_silver"},
    "for_hire_vehicle_trips": {"silver_suffix": "fhv_silver"},
    "high_volume_for_hire_vehicle_trips": { "silver_suffix": "hv_fhv_silver"}
}

# Comments for silver tables
# Comments for silver tables
TRIP_COMMENTS = {
    "yellow_silver": """
    This table contains individual trip records for New York City's iconic yellow taxis, collected from 2009 onward by authorized technology providers and submitted to the NYC Taxi and Limousine Commission (TLC). 
    Yellow taxis are the only vehicles permitted to pick up street hails citywide, and can also be booked via e-hail apps like Curb and Arro.
    Each row represents a single taxi ride and includes:

    - Temporal data: pickup_datetime, dropoff_datetime
    - Location details: pickup_location_id, dropoff_location_id (TLC Taxi Zones)
    - Trip metrics: passenger_count, trip_distance (in miles)
    - Fare breakdown: fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
    - Transaction metadata: ratecode_id, store_and_fwd_flag, payment_type, vendor_id.

    The data is submitted by licensed providers and may contain null or unknown values.
    """,
    "green_silver": """
    This table provides detailed trip-level data for NYC green taxis—also called boro taxis or street-hail liveries—launched in August 2013 to expand coverage in the outer boroughs and northern Manhattan. 
    These taxis may only respond to street hails in specific zones, unlike their yellow counterparts.
    Each row represents a single ride and includes:

    - Temporal data: lpep_pickup_datetime, lpep_dropoff_datetime
    - Location details: pu_location_id, do_location_id (TLC Taxi Zones)
    - Trip metrics: passenger_count, trip_distance
    - Fare breakdown: fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, cbd_congestion_fee
    - Transaction metadata: ratecode_id, store_and_fwd_flag, payment_type, vendor_id, trip_type.

    The data is submitted by licensed providers and may contain null or unknown values.
    """,
    "fhv_silver": """
    This table contains trip-level data for New York Citys For-Hire Vehicles (FHV), including records from high-volume dispatch bases (e.g., Uber, Lyft, Via, Juno), 
    as well as community livery, luxury limousine, and black car bases. These records have been submitted to the NYC Taxi and Limousine Commission (TLC) starting in 2015, with data availability expanding over time.
    Each row represents one dispatched trip and includes:

    - Dispatch and timing data: dispatching_base_num, pickup_datetime, dropoff_datetime
    - Location identifiers: pu_location_id, do_location_id (TLC Taxi Zones)
    - Shared ride indicator: sr_flag – indicates whether the ride was part of a shared trip
    - Vehicle affiliation info: affiliated_base_number
    High-volume licensees were formally categorized in 2019, but this dataset includes trips from both high-volume and non-high-volume FHV bases. 

    The data is submitted by licensed providers and may contain null or unknown values.
    """,
    "hv_fhv_silver": """
    This dataset contains trip-level records from NYCs High-Volume For-Hire Services (HVFHS), which include platforms like 
    Uber, Lyft, Via, and Juno. These services were officially categorized in 2019 under a new TLC license for companies
    dispatching over 10,000 daily trips.
    Each row represents a single ride and includes:

    - Licensing and base info: hvfhs_license_num, dispatching_base_num, originating_base_num
    - Trip timing and location: request_datetime, on_scene_datetime, pickup_datetime, dropoff_datetime, pu_location_id, do_location_id
    - Trip metrics and fares: trip_miles, trip_time, base_passenger_fare, tolls, tips, sales_tax, congestion_surcharge, airport_fee, driver_pay
    - Ride details: shared_request_flag, shared_match_flag, access_a_ride_flag, wav_request_flag, wav_match_flag, cbd_congestion_fee.

    The data is submitted by licensed providers and may contain null or unknown values.
    """
}

YELLOW_DICT = {
    "vendor_id": "A code indicating the TPEP provider that provided the record. 1 = Creative Mobile Technologies, LLC 2 = Curb Mobility, LLC 6 = Myle Technologies Inc 7 = Helix",
    "pickup_datetime": "The date and time when the meter was engaged.",
    "dropoff_datetime": "The date and time when the meter was disengaged.",
    "passenger_count": "The number of passengers in the vehicle.",
    "trip_distance": "The elapsed trip distance in miles reported by the taximeter.",
    "ratecode_id": "The final rate code in effect at the end of the trip. 1 = Standard rate 2 = JFK 3 = Newark 4 = Nassau or Westchester 5 = Negotiated fare 6 = Group ride 99 = Null/unknown",
    "store_and_fwd_flag": "This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka store and forward, because the vehicle did not have a connection to the server. Y = store and forward trip N = not a store and forward trip",
    "pu_location_id": "TLC Taxi Zone in which the taximeter was engaged.",
    "do_location_id": "TLC Taxi Zone in which the taximeter was disengaged.",
    "payment_type": "A numeric code signifying how the passenger paid for the trip. 0 = Flex Fare trip 1 = Credit card 2 = Cash 3 = No charge 4 = Dispute 5 = Unknown 6 = Voided trip",
    "fare_amount": "The time-and-distance fare calculated by the meter. For additional information on the following columns, see https://www.nyc.gov/site/tlc/passengers/taxi-fare.page",
    "extra": "Miscellaneous extras and surcharges.",
    "mta_tax": "Tax that is automatically triggered based on the metered rate in use.",
    "tip_amount": "Tip amount - This field is automatically populated for credit card tips. Cash tips are not included.",
    "tolls_amount": "Total amount of all tolls paid in trip.",
    "improvement_surcharge": "Improvement surcharge assessed trips at the flag drop. The improvement surcharge began being levied in 2015.",
    "total_amount": "The total amount charged to passengers. Does not include cash tips.",
    "airport_fee": "For pick up only at LaGuardia and John F. Kennedy Airports.",
    "congestion_surcharge": "Total amount collected in trip for NYS congestion surcharge.",
    "cbd_congestion_fee": "Per-trip charge for MTAs Congestion Relief Zone starting Jan. 5, 2025.",
    "year": "The year when the trip was recorded.",
    "month": "The month when the trip was recorded.",
    "ingestion_timestamp": "Date and time of ingestion data at silver layer.",
    "trip_type": "The type of the trip, it can be Yellow Taxi, Green Taxi, FHV or HVFHV."
}

GREEN_DICT = {
    "vendor_id": "A code indicating the LPEP provider that provided the record. 1 = Creative Mobile Technologies, LLC 2 = Curb Mobility, LLC 6 = Myle Technologies Inc",
    "pickup_datetime": "The date and time when the meter was engaged.",
    "dropoff_datetime": "The date and time when the meter was disengaged.",
    "store_and_fwd_flag": "This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka store and forward, because the vehicle did not have a connection to the server.",
    "ratecode_id": "The final rate code in effect at the end of the trip. 1 = Standard rate 2 = JFK 3 = Newark 4 = Nassau or Westchester 5 = Negotiated fare 6 = Group ride 99 = Null/unknown",
    "pu_location_id": "TLC Taxi Zone in which the taximeter was engaged.",
    "do_location_id": "TLC Taxi Zone in which the taximeter was disengaged.",
    "passenger_count": "The number of passengers in the vehicle.",
    "trip_distance": "The elapsed trip distance in miles reported by the taximeter.",
    "fare_amount": "The time-and-distance fare calculated by the meter. For additional information on the following columns, see https://www.nyc.gov/site/tlc/passengers/taxi-fare.page",
    "extra": "Miscellaneous extras and surcharges.",
    "mta_tax": "Tax that is automatically triggered based on the metered rate in use.",
    "tip_amount": "Tip amount - This field is automatically populated for credit card tips. Cash tips are not included.",
    "tolls_amount": "Total amount of all tolls paid in trip.",
    "ehail_fee":"No description available",
    "improvement_surcharge": "Improvement surcharge assessed trips at the flag drop. The improvement surcharge began being levied in 2015.",
    "total_amount": "The total amount charged to passengers. Does not include cash tips.",
    "payment_type": "A numeric code signifying how the passenger paid for the trip. 0 = Flex Fare trip 1 = Credit card 2 = Cash 3 = No charge 4 = Dispute 5 = Unknown 6 = Voided trip",
    "trip_type_code": "A code indicating whether the trip was a street-hail or a dispatch that is automatically assigned based on the metered rate in use but can be altered by the driver. 1 = Street-hail 2 = Dispatch",
    "congestion_surcharge": "Total amount collected in trip for NYS congestion surcharge.",
    "cbd_congestion_fee": "Per-trip charge for MTAs Congestion Relief Zone starting Jan. 5, 2025.",
    "year": "The year when the trip was recorded.",
    "month": "The month when the trip was recorded.",
    "ingestion_timestamp": "Date and time of ingestion data at silver layer.",
    "trip_type": "The type of the trip, it can be Yellow Taxi, Green Taxi, FHV or HVFHV."
}

FHV_DICT = {
    "vendor_id": "The TLC Base License Number of the base that dispatched the trip.",
    "pickup_datetime": "The date and time of the trip pick-up.",
    "dropoff_datetime": "The date and time of the trip dropoff.",
    "pu_location_id": "TLC Taxi Zone in which the trip began.",
    "do_location_id": "TLC Taxi Zone in which the trip ended.",
    "passenger_count": "The number of passengers in the vehicle.",
    "total_amount": "The total amount charged to passengers. Does not include cash tips.",
    "sr_flag": "Indicates if the trip was a part of a shared ride chain offered by a High Volume FHV company (e.g. Uber Pool, Lyft Line). For shared trips, the value is 1. For non-shared rides, this field is null. NOTE: For most High Volume FHV companies, only shared rides that were requested AND matched to another shared-ride request over the course of the journey are flagged. However, Lyft (base license numbers B02510 + B02844) also flags rides for which a shared ride was requested but another passenger was not successfully matched to share the trip—therefore, trips records with SR_Flag=1 from those two bases could indicate EITHER a first trip in a shared trip chain OR a trip for which a shared ride was requested but never matched. Users should anticipate an overcount of successfully shared trips completed by Lyft.",
    "affiliated_base_number": "Base number of the base with which the vehicle is affiliated. This must be provided even if the affiliated base is the same as the dispatching base.",
    "year": "The year when the trip was recorded.",
    "month": "The month when the trip was recorded.",
    "ingestion_timestamp": "Date and time of ingestion data at silver layer.",
    "trip_type": "The type of the trip, it can be Yellow Taxi, Green Taxi, FHV or HVFHV."
}

HVFHV_DICT = {
    "vendor_id": "The TLC license number of the HVFHS base or business. As of September 2019, the HVFHS licensees are the following: HV0002: Juno HV0003: Uber HV0004: Via HV0005: Lyft",
    "dispatching_base_num": "The TLC Base License Number of the base that dispatched the trip.",
    "originating_base_num": "Base number of the base that received the original trip request.",
    "request_datetime": "Date/time when passenger requested to be picked up.",
    "on_scene_datetime": "Date/time when driver arrived at the pick-up location (Accessible Vehicles-only).",
    "pickup_datetime": "The date and time of the trip pick-up.",
    "dropoff_datetime": "The date and time of the trip drop-off.",
    "passenger_count": "The number of passengers in the vehicle.",
    "pu_location_id": "TLC Taxi Zone in which the trip began.",
    "do_location_id": "TLC Taxi Zone in which the trip ended.",
    "trip_miles": "Total miles for passenger trip.",
    "trip_time": "Total time in seconds for passenger trip.",
    "base_passenger_fare": "Base passenger fare before tolls, tips, taxes, and fees.",
    "tolls": "Total amount of all tolls paid in trip.",
    "bcf": "Total amount collected in trip for Black Car Fund.",
    "sales_tax": "Total amount collected in trip for NYS sales tax.",
    "congestion_surcharge": "Total amount collected in trip for NYS congestion surcharge.",
    "airport_fee": "$2.50 for both drop off and pick up at LaGuardia, Newark, and John F. Kennedy airports.",
    "tips": "Total amount of tips received from passenger.",
    "driver_pay": "Total driver pay (not including tolls or tips and net of commission, surcharges, or taxes).",
    "shared_request_flag": "Did the passenger agree to a shared/pooled ride, regardless of whether they were matched? (Y/N)",
    "shared_match_flag": "Did the passenger share the vehicle with another passenger who booked separately at any point during the trip? (Y/N)",
    "access_a_ride_flag": "Was the trip administered on behalf of the Metropolitan Transportation Authority (MTA)? (Y/N)",
    "wav_request_flag": "Did the passenger request a wheelchair-accessible vehicle (WAV)? (Y/N)",
    "wav_match_flag": "Did the trip occur in a wheelchair-accessible vehicle (WAV)? (Y/N)",
    "cbd_congestion_fee": "Per-trip charge for MTAs Congestion Relief Zone starting Jan. 5, 2025.",
    "total_amount": "The total amount charged to passengers. Does not include cash tips.",
    "year": "The year when the trip was recorded.",
    "month": "The month when the trip was recorded.",
    "ingestion_timestamp": "Date and time of ingestion data at silver layer.",
    "trip_type": "The type of the trip, it can be Yellow Taxi, Green Taxi, FHV or HVFHV."
}




