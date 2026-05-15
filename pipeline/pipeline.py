# ============================================================
# NYC TAXI MANHATTAN PIPELINE
# Analisis Spasial dan Temporal Yellow Taxi vs HVFHV
# Wilayah: Manhattan Yellow Zone | Periode: Jan-Mar 2026
# ============================================================

from prefect import flow, task, get_run_logger
import requests
import duckdb
import pandas as pd
from pathlib import Path

# ============================================================
# CONSTANTS
# ============================================================

# Root project = folder induk dari pipeline/
ROOT_DIR  = Path(__file__).resolve().parent.parent
DATA_DIR  = ROOT_DIR / "data" / "raw"
ZONE_CSV  = ROOT_DIR / "taxi_zone_lookup.csv"
DB_PATH   = str(ROOT_DIR / "nyc_taxi_manhattan.duckdb")

BASE_URL  = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL  = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
MONTHS    = ["2026-01", "2026-02", "2026-03"]

MANHATTAN_YELLOW_ZONE_IDS = (
    4, 12, 13, 24, 43, 45, 48, 50, 68, 79,
    87, 88, 90, 100, 103, 104, 105, 107, 113, 114,
    125, 137, 140, 141, 142, 143, 144, 148, 151, 158,
    161, 162, 163, 164, 170, 186, 194, 209, 211, 224,
    229, 230, 231, 232, 233, 234, 236, 237, 238, 239,
    246, 249, 261, 262, 263
)

# ============================================================
# STAGE 1: INGESTION
# ============================================================

@task(name="Cek Zone Lookup")
def get_zone_lookup() -> Path:
    logger = get_run_logger()

    if ZONE_CSV.exists():
        logger.info(f"[SKIP] Zone lookup sudah ada: {ZONE_CSV}")
        return ZONE_CSV

    logger.info("[DOWNLOAD] Taxi zone lookup...")
    r = requests.get(ZONE_URL)
    r.raise_for_status()
    ZONE_CSV.write_text(r.text, encoding="utf-8")
    logger.info("[DONE] Zone lookup selesai didownload")
    return ZONE_CSV


@task(name="Cek / Download Parquet", retries=3, retry_delay_seconds=15)
def get_parquet(taxi_type: str, month: str) -> Path:
    logger = get_run_logger()
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    filename = f"{taxi_type}_tripdata_{month}.parquet"
    filepath = DATA_DIR / filename

    if filepath.exists():
        size_mb = filepath.stat().st_size / 1e6
        logger.info(f"[SKIP] File sudah ada: {filename} ({size_mb:.0f} MB)")
        return filepath

    url = f"{BASE_URL}/{filename}"
    logger.info(f"[DOWNLOAD] {url}")
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(filepath, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    size_mb = filepath.stat().st_size / 1e6
    logger.info(f"[DONE] {filename} ({size_mb:.1f} MB)")
    return filepath


# ============================================================
# STAGE 2: PREPROCESSING & CLEANING
# ============================================================

@task(name="Preprocessing Yellow Taxi")
def preprocess_yellow(parquet_files: list) -> Path:
    logger = get_run_logger()
    clean_dir = ROOT_DIR / "data" / "clean"
    clean_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    total_before = 0
    total_after  = 0

    for filepath in parquet_files:
        filepath  = Path(filepath)
        out_path  = clean_dir / filepath.name
        logger.info(f"[PREPROCESSING] {filepath.name}")

        # Hitung BEFORE
        before = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{filepath.as_posix()}')"
        ).fetchone()[0]
        total_before += before

        # Cleaning:
        # 1. Filter zona Manhattan Yellow Zone
        # 2. Buang fare <= 0
        # 3. Buang jarak = 0 atau > 100 mil
        # 4. Buang waktu dropoff <= pickup (waktu terbalik)
        # 5. Buang durasi < 1 menit atau > 180 menit
        # 6. Isi missing value dengan 0 (COALESCE)
        # 7. Rename kolom agar konsisten dengan FHVHV
        con.execute(f"""
            COPY (
                SELECT
                    CAST(VendorID AS VARCHAR)             AS operator_code,
                    PULocationID,
                    DOLocationID,
                    tpep_pickup_datetime                  AS pickup_datetime,
                    tpep_dropoff_datetime                 AS dropoff_datetime,
                    ROUND(DATEDIFF('second',
                        tpep_pickup_datetime,
                        tpep_dropoff_datetime) / 60.0, 2) AS trip_duration_min,
                    trip_distance,
                    fare_amount,
                    tip_amount,
                    tolls_amount,
                    COALESCE(congestion_surcharge, 0)     AS congestion_surcharge,
                    COALESCE(Airport_fee, 0)              AS airport_fee,
                    COALESCE(cbd_congestion_fee, 0)       AS cbd_congestion_fee,
                    total_amount
                FROM read_parquet('{filepath.as_posix()}')
                WHERE PULocationID  IN {MANHATTAN_YELLOW_ZONE_IDS}
                  AND DOLocationID  IN {MANHATTAN_YELLOW_ZONE_IDS}
                  AND fare_amount    > 0
                  AND trip_distance  > 0
                  AND trip_distance  < 100
                  AND tpep_dropoff_datetime > tpep_pickup_datetime
                  AND DATEDIFF('second',
                        tpep_pickup_datetime,
                        tpep_dropoff_datetime) BETWEEN 60 AND 10800
            ) TO '{out_path.as_posix()}' (FORMAT PARQUET)
        """)

        after = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out_path.as_posix()}')"
        ).fetchone()[0]
        total_after += after

        dibuang = before - after
        logger.info(f"  Before : {before:>10,} trip")
        logger.info(f"  After  : {after:>10,} trip")
        logger.info(f"  Dibuang: {dibuang:>10,} baris ({dibuang/before*100:.1f}%)")

    con.close()
    logger.info(f"[DONE] Yellow — Total before: {total_before:,} | after: {total_after:,}")
    return clean_dir


@task(name="Preprocessing FHVHV")
def preprocess_fhvhv(parquet_files: list) -> Path:
    logger = get_run_logger()
    clean_dir = ROOT_DIR / "data" / "clean"
    clean_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    total_before = 0
    total_after  = 0

    for filepath in parquet_files:
        filepath = Path(filepath)
        out_path = clean_dir / filepath.name
        logger.info(f"[PREPROCESSING] {filepath.name}")

        before = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{filepath.as_posix()}')"
        ).fetchone()[0]
        total_before += before

        # Cleaning:
        # 1. Filter zona Manhattan Yellow Zone
        # 2. Buang fare <= 0
        # 3. Buang jarak = 0 atau > 100 mil
        # 4. Buang waktu terbalik
        # 5. Buang durasi tidak wajar
        # 6. Hitung total_amount dari komponen-komponennya
        # 7. Rename kolom agar konsisten dengan Yellow
        con.execute(f"""
            COPY (
                SELECT
                    hvfhs_license_num                      AS operator_code,
                    PULocationID,
                    DOLocationID,
                    pickup_datetime,
                    dropoff_datetime,
                    ROUND(DATEDIFF('second',
                        pickup_datetime,
                        dropoff_datetime) / 60.0, 2)       AS trip_duration_min,
                    trip_miles                             AS trip_distance,
                    base_passenger_fare                    AS fare_amount,
                    tips                                   AS tip_amount,
                    tolls                                  AS tolls_amount,
                    COALESCE(congestion_surcharge, 0)      AS congestion_surcharge,
                    COALESCE(airport_fee, 0)               AS airport_fee,
                    COALESCE(cbd_congestion_fee, 0)        AS cbd_congestion_fee,
                    ROUND(base_passenger_fare + tolls + bcf +
                          sales_tax + congestion_surcharge +
                          airport_fee + tips +
                          cbd_congestion_fee, 2)           AS total_amount
                FROM read_parquet('{filepath.as_posix()}')
                WHERE PULocationID      IN {MANHATTAN_YELLOW_ZONE_IDS}
                  AND DOLocationID      IN {MANHATTAN_YELLOW_ZONE_IDS}
                  AND base_passenger_fare > 0
                  AND trip_miles          > 0
                  AND trip_miles          < 100
                  AND dropoff_datetime  > pickup_datetime
                  AND DATEDIFF('second',
                        pickup_datetime,
                        dropoff_datetime) BETWEEN 60 AND 10800
            ) TO '{out_path.as_posix()}' (FORMAT PARQUET)
        """)

        after = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out_path.as_posix()}')"
        ).fetchone()[0]
        total_after  += after

        dibuang = before - after
        logger.info(f"  Before : {before:>10,} trip")
        logger.info(f"  After  : {after:>10,} trip")
        logger.info(f"  Dibuang: {dibuang:>10,} baris ({dibuang/before*100:.1f}%)")

    con.close()
    logger.info(f"[DONE] FHVHV — Total before: {total_before:,} | after: {total_after:,}")
    return clean_dir


# ============================================================
# STAGE 3: STORAGE
# ============================================================

@task(name="Setup DuckDB Schema")
def setup_schema(zone_path: Path) -> str:
    logger = get_run_logger()
    con = duckdb.connect(DB_PATH)

    # -- dim_location --
    con.execute("DROP TABLE IF EXISTS dim_location")
    zones     = pd.read_csv(zone_path)
    zones_myz = zones[zones["LocationID"].isin(MANHATTAN_YELLOW_ZONE_IDS)].copy()
    con.execute("""
        CREATE TABLE dim_location AS
        SELECT
            LocationID   AS location_id,
            Zone         AS zone_name,
            Borough      AS borough,
            service_zone
        FROM zones_myz
    """)
    logger.info(f"[dim_location] {len(zones_myz)} zona Manhattan Yellow Zone")

    # -- dim_operator --
    con.execute("DROP TABLE IF EXISTS dim_operator")
    con.execute("""
        CREATE TABLE dim_operator AS
        SELECT * FROM (VALUES
            (1, '1',      'Creative Mobile Tech', 'yellow', 'Konvensional'),
            (2, '2',      'VeriFone',             'yellow', 'Konvensional'),
            (3, 'HV0003', 'Uber',                 'fhvhv',  'Modern'),
            (4, 'HV0005', 'Lyft',                 'fhvhv',  'Modern')
        ) t(operator_id, operator_code, operator_name, taxi_type, category)
    """)
    logger.info("[dim_operator] 4 operator terdaftar")

    # -- fact_trips --
    con.execute("DROP TABLE IF EXISTS fact_trips")
    con.execute("""
        CREATE TABLE fact_trips (
            trip_id              BIGINT,
            operator_code        VARCHAR,
            PULocationID         INT,
            DOLocationID         INT,
            pickup_datetime      TIMESTAMP,
            dropoff_datetime     TIMESTAMP,
            trip_duration_min    FLOAT,
            trip_distance        FLOAT,
            fare_amount          FLOAT,
            tip_amount           FLOAT,
            tolls_amount         FLOAT,
            congestion_surcharge FLOAT,
            airport_fee          FLOAT,
            cbd_congestion_fee   FLOAT,
            total_amount         FLOAT
        )
    """)
    logger.info("[fact_trips] Tabel kosong siap diisi")

    con.close()
    return DB_PATH


@task(name="Load Yellow Taxi Trips")
def load_yellow_trips(clean_dir: Path) -> int:
    logger = get_run_logger()
    con    = duckdb.connect(DB_PATH)

    files = sorted(Path(clean_dir).glob("yellow_tripdata_*.parquet"))
    for filepath in files:
        logger.info(f"[LOAD → DB] {filepath.name}")

        offset = con.execute(
            "SELECT COALESCE(MAX(trip_id), 0) FROM fact_trips"
        ).fetchone()[0]

        con.execute(f"""
            INSERT INTO fact_trips
            SELECT
                ROW_NUMBER() OVER () + {offset} AS trip_id,
                operator_code,
                PULocationID,
                DOLocationID,
                pickup_datetime,
                dropoff_datetime,
                trip_duration_min,
                trip_distance,
                fare_amount,
                tip_amount,
                tolls_amount,
                congestion_surcharge,
                airport_fee,
                cbd_congestion_fee,
                total_amount
            FROM read_parquet('{filepath.as_posix()}')
        """)

        count = con.execute(
            "SELECT COUNT(*) FROM fact_trips"
        ).fetchone()[0]
        logger.info(f"  Akumulasi fact_trips: {count:,} baris")

    total = con.execute(
        "SELECT COUNT(*) FROM fact_trips WHERE operator_code IN ('1', '2')"
    ).fetchone()[0]
    con.close()
    logger.info(f"[DONE] Yellow total di DB: {total:,} trip")
    return total


@task(name="Load FHVHV Trips")
def load_fhvhv_trips(clean_dir: Path) -> int:
    logger = get_run_logger()
    con    = duckdb.connect(DB_PATH)

    files = sorted(Path(clean_dir).glob("fhvhv_tripdata_*.parquet"))
    for filepath in files:
        logger.info(f"[LOAD → DB] {filepath.name}")

        offset = con.execute(
            "SELECT COALESCE(MAX(trip_id), 0) FROM fact_trips"
        ).fetchone()[0]

        con.execute(f"""
            INSERT INTO fact_trips
            SELECT
                ROW_NUMBER() OVER () + {offset} AS trip_id,
                operator_code,
                PULocationID,
                DOLocationID,
                pickup_datetime,
                dropoff_datetime,
                trip_duration_min,
                trip_distance,
                fare_amount,
                tip_amount,
                tolls_amount,
                congestion_surcharge,
                airport_fee,
                cbd_congestion_fee,
                total_amount
            FROM read_parquet('{filepath.as_posix()}')
        """)

        count = con.execute(
            "SELECT COUNT(*) FROM fact_trips"
        ).fetchone()[0]
        logger.info(f"  Akumulasi fact_trips: {count:,} baris")

    total = con.execute(
        "SELECT COUNT(*) FROM fact_trips WHERE operator_code IN ('HV0003', 'HV0005')"
    ).fetchone()[0]
    con.close()
    logger.info(f"[DONE] FHVHV total di DB: {total:,} trip")
    return total


# ============================================================
# STAGE 4: ANALISIS - AGGREGASI
# ============================================================

@task(name="Buat Tabel Agregasi")
def create_aggregation() -> dict:
    logger = get_run_logger()
    con = duckdb.connect(DB_PATH)

    # -- agg_hourly_demand: volume & heatmap per jam --
    con.execute("DROP TABLE IF EXISTS agg_hourly_demand")
    con.execute("""
        CREATE TABLE agg_hourly_demand AS
        SELECT
            DATE(f.pickup_datetime)                AS date,
            HOUR(f.pickup_datetime)                AS hour,
            DAYOFWEEK(f.pickup_datetime)           AS day_of_week,
            f.PULocationID,
            o.operator_id,
            o.taxi_type,
            o.category,
            o.operator_name,
            COUNT(*)                               AS trip_count,
            ROUND(AVG(f.fare_amount),       2)     AS avg_fare,
            ROUND(AVG(f.total_amount),      2)     AS avg_total,
            ROUND(AVG(f.trip_duration_min), 2)     AS avg_duration_min,
            ROUND(AVG(f.trip_distance),     2)     AS avg_distance,
            ROUND(AVG(f.tip_amount),        2)     AS avg_tip
        FROM fact_trips f
        JOIN dim_operator o ON f.operator_code = o.operator_code
        GROUP BY
            DATE(f.pickup_datetime),
            HOUR(f.pickup_datetime),
            DAYOFWEEK(f.pickup_datetime),
            f.PULocationID,
            o.operator_id, o.taxi_type, o.category, o.operator_name
    """)

    # -- agg_zone_demand: intensitas per zona (untuk peta) --
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
            COUNT(*)                               AS trip_count,
            ROUND(AVG(f.fare_amount),       2)     AS avg_fare,
            ROUND(AVG(f.trip_distance),     2)     AS avg_distance,
            ROUND(AVG(f.trip_duration_min), 2)     AS avg_duration_min,
            ROUND(AVG(f.tip_amount),        2)     AS avg_tip
        FROM fact_trips f
        JOIN dim_operator o ON f.operator_code = o.operator_code
        JOIN dim_location l ON f.PULocationID  = l.location_id
        GROUP BY
            DATE_TRUNC('month', f.pickup_datetime),
            f.PULocationID, l.zone_name, l.borough,
            o.taxi_type, o.category, o.operator_name
    """)

    hourly = con.execute("SELECT COUNT(*) FROM agg_hourly_demand").fetchone()[0]
    zone   = con.execute("SELECT COUNT(*) FROM agg_zone_demand").fetchone()[0]
    logger.info(f"[agg_hourly_demand] {hourly:,} baris")
    logger.info(f"[agg_zone_demand]   {zone:,} baris")

    con.close()
    return {"agg_hourly_demand": hourly, "agg_zone_demand": zone}


# ============================================================
# VALIDASI
# ============================================================

@task(name="Validasi Hasil Pipeline")
def validate_pipeline() -> dict:
    logger = get_run_logger()
    con = duckdb.connect(DB_PATH)

    result = {}
    tables = ["dim_location", "dim_operator", "fact_trips",
              "agg_hourly_demand", "agg_zone_demand"]

    for table in tables:
        n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        result[table] = n
        logger.info(f"  {table:25s}: {n:>12,} baris")

    dist = con.execute("""
        SELECT o.category, o.taxi_type, o.operator_name, COUNT(*) AS total_trip
        FROM fact_trips f
        JOIN dim_operator o ON f.operator_code = o.operator_code
        GROUP BY o.category, o.taxi_type, o.operator_name
        ORDER BY total_trip DESC
    """).df()
    logger.info(f"\nDistribusi per operator:\n{dist.to_string(index=False)}")

    con.close()
    return result


# ============================================================
# MAIN FLOW
# ============================================================

@flow(name="NYC Taxi Manhattan Pipeline", log_prints=True)
def nyc_taxi_pipeline(months: list = MONTHS):
    logger = get_run_logger()
    logger.info(f"Pipeline dimulai | Bulan: {months}")
    logger.info(f"Root dir : {ROOT_DIR}")
    logger.info(f"Data dir : {DATA_DIR}")
    logger.info(f"Database : {DB_PATH}")

    # STAGE 1 — Ingestion
    zone_path    = get_zone_lookup()
    yellow_files = [get_parquet("yellow", m) for m in months]
    fhvhv_files  = [get_parquet("fhvhv",  m) for m in months]

    # STAGE 2 — Preprocessing & Cleaning
    clean_dir_yellow = preprocess_yellow(yellow_files)
    clean_dir_fhvhv  = preprocess_fhvhv(fhvhv_files)

    # STAGE 3 — Storage (load clean data ke DuckDB)
    setup_schema(zone_path)
    yellow_count = load_yellow_trips(clean_dir_yellow)
    fhvhv_count  = load_fhvhv_trips(clean_dir_fhvhv)

    # STAGE 4 — Analisis & Agregasi
    create_aggregation()

    # Validasi akhir
    result = validate_pipeline()

    logger.info(f"""
    ============================================
    PIPELINE SELESAI
    Yellow Taxi : {yellow_count:,} trip
    FHVHV       : {fhvhv_count:,} trip
    Total       : {yellow_count + fhvhv_count:,} trip
    Database    : {DB_PATH}
    ============================================
    """)
    return result


if __name__ == "__main__":
    # Jalankan pipeline sekali
    nyc_taxi_pipeline()

    # Untuk scheduling otomatis, ganti baris di atas dengan:
    # nyc_taxi_pipeline.serve(
    #     name="nyc-taxi-pipeline",
    #     cron="0 2 * * *"    # setiap hari jam 02:00
    # )
