# Anggota Kelompok:
# 1. 235150201111005 - Farhaan Habibi
# 2. 235150201111010 - Muhamad Fa'iz Al Akbar
# 3. 235150201111011 - Rafly Januar Raharjo
# 4. 235150201111012 - Arif Rahman
# 5. 235150207111012 - Yoshia Benedict Parasian

from pathlib import Path
import requests
from prefect import task, get_run_logger

from pipeline.config import DATA_DIR, ZONE_CSV, BASE_URL, ZONE_URL


@task(name="Cek Zone Lookup")
def get_zone_lookup() -> Path:
    logger = get_run_logger()

    if ZONE_CSV.exists():
        logger.info(f"[SKIP] Zone lookup sudah ada: {ZONE_CSV}")
        return ZONE_CSV

    logger.info("[DOWNLOAD] Taxi zone lookup...")
    response = requests.get(ZONE_URL)
    response.raise_for_status()

    ZONE_CSV.write_text(response.text, encoding="utf-8")

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

    with open(filepath, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)

    size_mb = filepath.stat().st_size / 1e6
    logger.info(f"[DONE] {filename} ({size_mb:.1f} MB)")

    return filepath
