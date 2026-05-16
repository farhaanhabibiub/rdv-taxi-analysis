# Anggota Kelompok:
# 1. 235150201111005 - Farhaan Habibi
# 2. 235150201111010 - Muhamad Fa'iz Al Akbar
# 3. 235150201111011 - Rafly Januar Raharjo
# 4. 235150201111012 - Arif Rahman
# 5. 235150207111012 - Yoshia Benedict Parasian

from prefect import flow, get_run_logger

from pipeline.config import MONTHS
from pipeline.ingestion import get_zone_lookup, get_parquet
from pipeline.cleaning import preprocess_yellow, preprocess_fhvhv
from pipeline.transform import setup_schema, load_yellow_trips, load_fhvhv_trips
from pipeline.analysis import create_aggregation, validate_pipeline
from pipeline.weather import fetch_weather_data
from pipeline.ml_demand import train_demand_model


@flow(name="NYC Taxi Manhattan Pipeline", log_prints=True)
def nyc_taxi_pipeline(months: list = MONTHS):
    logger = get_run_logger()

    logger.info(f"Pipeline dimulai untuk bulan: {months}")

    zone_path = get_zone_lookup()

    yellow_files = [get_parquet("yellow", month) for month in months]
    fhvhv_files = [get_parquet("fhvhv", month) for month in months]

    clean_dir_yellow = preprocess_yellow(yellow_files)
    clean_dir_fhvhv = preprocess_fhvhv(fhvhv_files)

    setup_schema(zone_path)

    yellow_count = load_yellow_trips(clean_dir_yellow)
    fhvhv_count = load_fhvhv_trips(clean_dir_fhvhv)

    fetch_weather_data()

    create_aggregation()

    train_demand_model()

    result = validate_pipeline()

    logger.info(f"""
    =====================================
    PIPELINE SELESAI
    Yellow Taxi : {yellow_count:,} trip
    FHVHV       : {fhvhv_count:,} trip
    Total       : {yellow_count + fhvhv_count:,} trip
    =====================================
    """)

    return result


if __name__ == "__main__":
    nyc_taxi_pipeline()
