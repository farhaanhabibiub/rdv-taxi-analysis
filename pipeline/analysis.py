# Anggota Kelompok:
# 1. 235150201111005 - Farhaan Habibi
# 2. 235150201111010 - Muhamad Fa'iz Al Akbar
# 3. 235150201111011 - Rafly Januar Raharjo
# 4. 235150201111012 - Arif Rahman
# 5. 235150207111012 - Yoshia Benedict Parasian

import duckdb
from prefect import task, get_run_logger

from pipeline.config import DB_PATH


@task(name="Buat Tabel Agregasi")
def create_aggregation() -> dict:
    logger = get_run_logger()
    con = duckdb.connect(DB_PATH)

    con.execute("DROP TABLE IF EXISTS agg_hourly_demand")
    con.execute("""
        CREATE TABLE agg_hourly_demand AS
        SELECT
            DATE(f.pickup_datetime) AS date,
            HOUR(f.pickup_datetime) AS hour,
            DAYOFWEEK(f.pickup_datetime) AS day_of_week,
            f.PULocationID,
            o.operator_id,
            o.taxi_type,
            o.category,
            o.operator_name,
            COUNT(*) AS trip_count,
            ROUND(AVG(f.fare_amount), 2) AS avg_fare,
            ROUND(AVG(f.total_amount), 2) AS avg_total,
            ROUND(AVG(f.trip_duration_min), 2) AS avg_duration_min,
            ROUND(AVG(f.trip_distance), 2) AS avg_distance,
            ROUND(AVG(f.tip_amount), 2) AS avg_tip
        FROM fact_trips f
        JOIN dim_operator o ON f.operator_code = o.operator_code
        GROUP BY
            DATE(f.pickup_datetime),
            HOUR(f.pickup_datetime),
            DAYOFWEEK(f.pickup_datetime),
            f.PULocationID,
            o.operator_id,
            o.taxi_type,
            o.category,
            o.operator_name
    """)

    con.execute("DROP TABLE IF EXISTS agg_zone_demand")
    con.execute("""
        CREATE TABLE agg_zone_demand AS
        SELECT
            DATE_TRUNC('month', f.pickup_datetime) AS month,
            f.PULocationID,
            l.zone_name,
            l.borough,
            o.taxi_type,
            o.category,
            o.operator_name,
            COUNT(*) AS trip_count,
            ROUND(AVG(f.fare_amount), 2) AS avg_fare,
            ROUND(AVG(f.trip_distance), 2) AS avg_distance,
            ROUND(AVG(f.trip_duration_min), 2) AS avg_duration_min,
            ROUND(AVG(f.tip_amount), 2) AS avg_tip
        FROM fact_trips f
        JOIN dim_operator o ON f.operator_code = o.operator_code
        JOIN dim_location l ON f.PULocationID = l.location_id
        GROUP BY
            DATE_TRUNC('month', f.pickup_datetime),
            f.PULocationID,
            l.zone_name,
            l.borough,
            o.taxi_type,
            o.category,
            o.operator_name
    """)

    hourly = con.execute("SELECT COUNT(*) FROM agg_hourly_demand").fetchone()[0]
    zone = con.execute("SELECT COUNT(*) FROM agg_zone_demand").fetchone()[0]

    logger.info(f"agg_hourly_demand: {hourly:,} baris")
    logger.info(f"agg_zone_demand: {zone:,} baris")

    con.close()

    return {
        "agg_hourly_demand": hourly,
        "agg_zone_demand": zone
    }


@task(name="Validasi Hasil Pipeline")
def validate_pipeline() -> dict:
    logger = get_run_logger()
    con = duckdb.connect(DB_PATH)

    tables = [
        "dim_location",
        "dim_operator",
        "fact_trips",
        "agg_hourly_demand",
        "agg_zone_demand"
    ]

    result = {}

    for table in tables:
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        result[table] = count
        logger.info(f"{table}: {count:,} baris")

    con.close()
    return result
