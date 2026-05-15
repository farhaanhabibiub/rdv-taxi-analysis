from prefect import flow, get_run_logger

from pipeline.config import MONTHS
from pipeline.ingestion import get_zone_lookup, get_parquet
from pipeline.cleaning import preprocess_yellow, preprocess_fhvhv
from pipeline.transform import setup_schema, load_yellow_trips, load_fhvhv_trips
from pipeline.analysis import create_aggregation, validate_pipeline


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

    create_aggregation()

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