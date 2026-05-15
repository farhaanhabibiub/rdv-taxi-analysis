# Anggota Kelompok:
# 1. 235150201111005 - Farhaan Habibi
# 2. 235150201111010 - Muhamad Fa'iz Al Akbar
# 3. 235150201111011 - Rafly Januar Raharjo
# 4. 235150201111012 - Arif Rahman
# 5. 235150207111012 - Yoshia Benedict Parasian

from pathlib import Path
import pandas as pd
import duckdb
from prefect import task, get_run_logger

from pipeline.config import DB_PATH, MANHATTAN_YELLOW_ZONE_IDS


@task(name="Setup DuckDB Schema")
def setup_schema(zone_path: Path) -> str:
    logger = get_run_logger()
    con = duckdb.connect(DB_PATH)

    con.execute("DROP TABLE IF EXISTS dim_location")

    zones = pd.read_csv(zone_path)
    zones_myz = zones[zones["LocationID"].isin(MANHATTAN_YELLOW_ZONE_IDS)].copy()

    con.execute("""
        CREATE TABLE dim_location AS
        SELECT
            LocationID AS location_id,
            Zone AS zone_name,
            Borough AS borough,
            service_zone
        FROM zones_myz
    """)

    con.execute("DROP TABLE IF EXISTS dim_operator")
    con.execute("""
        CREATE TABLE dim_operator AS
        SELECT * FROM (VALUES
            (1, '1', 'Creative Mobile Tech', 'yellow', 'Konvensional'),
            (2, '2', 'VeriFone', 'yellow', 'Konvensional'),
            (3, 'HV0003', 'Uber', 'fhvhv', 'Modern'),
            (4, 'HV0005', 'Lyft', 'fhvhv', 'Modern')
        ) t(operator_id, operator_code, operator_name, taxi_type, category)
    """)

    con.execute("DROP TABLE IF EXISTS fact_trips")
    con.execute("""
        CREATE TABLE fact_trips (
            trip_id BIGINT,
            operator_code VARCHAR,
            PULocationID INT,
            DOLocationID INT,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            trip_duration_min FLOAT,
            trip_distance FLOAT,
            fare_amount FLOAT,
            tip_amount FLOAT,
            tolls_amount FLOAT,
            congestion_surcharge FLOAT,
            airport_fee FLOAT,
            cbd_congestion_fee FLOAT,
            total_amount FLOAT
        )
    """)

    logger.info("[DONE] Schema DuckDB berhasil dibuat")
    con.close()

    return DB_PATH


@task(name="Load Yellow Taxi Trips")
def load_yellow_trips(clean_dir: Path) -> int:
    logger = get_run_logger()
    con = duckdb.connect(DB_PATH)

    files = sorted(Path(clean_dir).glob("yellow_tripdata_*.parquet"))

    for filepath in files:
        logger.info(f"[LOAD] {filepath.name}")

        offset = con.execute("SELECT COALESCE(MAX(trip_id), 0) FROM fact_trips").fetchone()[0]

        con.execute(f"""
            INSERT INTO fact_trips
            SELECT
                ROW_NUMBER() OVER () + {offset} AS trip_id,
                *
            FROM read_parquet('{filepath.as_posix()}')
        """)

    total = con.execute("""
        SELECT COUNT(*) FROM fact_trips
        WHERE operator_code IN ('1', '2')
    """).fetchone()[0]

    con.close()
    return total


@task(name="Load FHVHV Trips")
def load_fhvhv_trips(clean_dir: Path) -> int:
    logger = get_run_logger()
    con = duckdb.connect(DB_PATH)

    files = sorted(Path(clean_dir).glob("fhvhv_tripdata_*.parquet"))

    for filepath in files:
        logger.info(f"[LOAD] {filepath.name}")

        offset = con.execute("SELECT COALESCE(MAX(trip_id), 0) FROM fact_trips").fetchone()[0]

        con.execute(f"""
            INSERT INTO fact_trips
            SELECT
                ROW_NUMBER() OVER () + {offset} AS trip_id,
                *
            FROM read_parquet('{filepath.as_posix()}')
        """)

    total = con.execute("""
        SELECT COUNT(*) FROM fact_trips
        WHERE operator_code IN ('HV0003', 'HV0005')
    """).fetchone()[0]

    con.close()
    return total
