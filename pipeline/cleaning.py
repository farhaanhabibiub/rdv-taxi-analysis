# Anggota Kelompok:
# 1. 235150201111005 - Farhaan Habibi
# 2. 235150201111010 - Muhamad Fa'iz Al Akbar
# 3. 235150201111011 - Rafly Januar Raharjo
# 4. 235150201111012 - Arif Rahman
# 5. 235150207111012 - Yoshia Benedict Parasian

from pathlib import Path
import duckdb
from prefect import task, get_run_logger

from pipeline.config import CLEAN_DIR, MANHATTAN_YELLOW_ZONE_IDS


@task(name="Preprocessing Yellow Taxi")
def preprocess_yellow(parquet_files: list) -> Path:
    logger = get_run_logger()
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    for filepath in parquet_files:
        filepath = Path(filepath)
        out_path = CLEAN_DIR / filepath.name

        logger.info(f"[PREPROCESSING] {filepath.name}")

        con.execute(f"""
            COPY (
                SELECT
                    CAST(VendorID AS VARCHAR) AS operator_code,
                    PULocationID,
                    DOLocationID,
                    tpep_pickup_datetime AS pickup_datetime,
                    tpep_dropoff_datetime AS dropoff_datetime,
                    ROUND(DATEDIFF('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0, 2) AS trip_duration_min,
                    trip_distance,
                    fare_amount,
                    tip_amount,
                    tolls_amount,
                    COALESCE(congestion_surcharge, 0) AS congestion_surcharge,
                    COALESCE(Airport_fee, 0) AS airport_fee,
                    COALESCE(cbd_congestion_fee, 0) AS cbd_congestion_fee,
                    total_amount
                FROM read_parquet('{filepath.as_posix()}')
                WHERE PULocationID IN {MANHATTAN_YELLOW_ZONE_IDS}
                  AND DOLocationID IN {MANHATTAN_YELLOW_ZONE_IDS}
                  AND fare_amount > 0
                  AND trip_distance > 0
                  AND trip_distance < 100
                  AND tpep_dropoff_datetime > tpep_pickup_datetime
                  AND DATEDIFF('second', tpep_pickup_datetime, tpep_dropoff_datetime) BETWEEN 60 AND 10800
            ) TO '{out_path.as_posix()}' (FORMAT PARQUET)
        """)

    con.close()
    return CLEAN_DIR


@task(name="Preprocessing FHVHV")
def preprocess_fhvhv(parquet_files: list) -> Path:
    logger = get_run_logger()
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    for filepath in parquet_files:
        filepath = Path(filepath)
        out_path = CLEAN_DIR / filepath.name

        logger.info(f"[PREPROCESSING] {filepath.name}")

        con.execute(f"""
            COPY (
                SELECT
                    hvfhs_license_num AS operator_code,
                    PULocationID,
                    DOLocationID,
                    pickup_datetime,
                    dropoff_datetime,
                    ROUND(DATEDIFF('second', pickup_datetime, dropoff_datetime) / 60.0, 2) AS trip_duration_min,
                    trip_miles AS trip_distance,
                    base_passenger_fare AS fare_amount,
                    tips AS tip_amount,
                    tolls AS tolls_amount,
                    COALESCE(congestion_surcharge, 0) AS congestion_surcharge,
                    COALESCE(airport_fee, 0) AS airport_fee,
                    COALESCE(cbd_congestion_fee, 0) AS cbd_congestion_fee,
                    ROUND(base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + airport_fee + tips + cbd_congestion_fee, 2) AS total_amount
                FROM read_parquet('{filepath.as_posix()}')
                WHERE PULocationID IN {MANHATTAN_YELLOW_ZONE_IDS}
                  AND DOLocationID IN {MANHATTAN_YELLOW_ZONE_IDS}
                  AND base_passenger_fare > 0
                  AND trip_miles > 0
                  AND trip_miles < 100
                  AND dropoff_datetime > pickup_datetime
                  AND DATEDIFF('second', pickup_datetime, dropoff_datetime) BETWEEN 60 AND 10800
            ) TO '{out_path.as_posix()}' (FORMAT PARQUET)
        """)

    con.close()
    return CLEAN_DIR
