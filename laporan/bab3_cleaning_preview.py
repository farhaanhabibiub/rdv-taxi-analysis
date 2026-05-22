import duckdb
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

con = duckdb.connect()

# =========================================================
# RAW DATA
# =========================================================

df_raw = con.execute("""
SELECT *
FROM read_parquet('data/raw/yellow_tripdata_2026-01.parquet')
LIMIT 100000
""").df()

print("\n================ BEFORE CLEANING : INFO =================\n")
print(df_raw.info())

print("\n================ BEFORE CLEANING : DESCRIBE =================\n")
print(df_raw.describe())

# =========================================================
# MISSING VALUES
# =========================================================

print("\n================ MISSING VALUES =================\n")
print(df_raw.isnull().sum())

# =========================================================
# ANOMALIES
# =========================================================

print("\n================ NEGATIVE FARE =================\n")

negative_fare = df_raw[df_raw["fare_amount"] <= 0]

print(negative_fare.head(10))

print("\n================ INVALID DISTANCE =================\n")

invalid_distance = df_raw[df_raw["trip_distance"] <= 0]

print(invalid_distance.head(10))

# =========================================================
# CLEANING PROCESS
# =========================================================

df_clean = con.execute("""
SELECT
    *,

    datediff(
        'minute',
        tpep_pickup_datetime,
        tpep_dropoff_datetime
    ) AS trip_duration_min,

    EXTRACT(hour FROM tpep_pickup_datetime) AS hour,

    dayname(tpep_pickup_datetime) AS day_of_week

FROM read_parquet('data/raw/yellow_tripdata_2026-01.parquet')

WHERE fare_amount > 0
AND trip_distance > 0
AND total_amount > 0
AND PULocationID IS NOT NULL
AND DOLocationID IS NOT NULL

AND datediff(
    'minute',
    tpep_pickup_datetime,
    tpep_dropoff_datetime
) BETWEEN 1 AND 180

LIMIT 100000
""").df()

# =========================================================
# AFTER CLEANING
# =========================================================

print("\n================ AFTER CLEANING : INFO =================\n")
print(df_clean.info())

print("\n================ AFTER CLEANING : DESCRIBE =================\n")
print(df_clean.describe())

# =========================================================
# DERIVED COLUMNS
# =========================================================

print("\n================ DERIVED COLUMNS =================\n")

print(
    df_clean[
        [
            "tpep_pickup_datetime",
            "trip_duration_min",
            "hour",
            "day_of_week",
            "trip_distance",
            "fare_amount"
        ]
    ].head(10)
)