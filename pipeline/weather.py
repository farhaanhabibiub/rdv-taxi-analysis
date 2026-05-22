# Anggota Kelompok:
# 1. 235150201111005 - Farhaan Habibi
# 2. 235150201111010 - Muhamad Fa'iz Al Akbar
# 3. 235150201111011 - Rafly Januar Raharjo
# 4. 235150201111012 - Arif Rahman
# 5. 235150207111012 - Yoshia Benedict Parasian

import requests
import pandas as pd
import duckdb
from prefect import task, get_run_logger

from pipeline.config import DB_PATH

NYC_LAT   = 40.7128
NYC_LON   = -74.0060
START_DATE = "2026-01-01"
END_DATE   = "2026-03-31"

HOURLY_VARS = ",".join([
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "snowfall",
    "windspeed_10m",
    "weathercode",
])


@task(name="Fetch Data Cuaca NYC (Open-Meteo)")
def fetch_weather_data() -> int:
    logger = get_run_logger()

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude":   NYC_LAT,
        "longitude":  NYC_LON,
        "start_date": START_DATE,
        "end_date":   END_DATE,
        "hourly":     HOURLY_VARS,
        "timezone":   "America/New_York",
    }

    logger.info(f"Mengunduh data cuaca Open-Meteo: {START_DATE} — {END_DATE}")
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    h = r.json()["hourly"]

    df = pd.DataFrame({
        "datetime":     pd.to_datetime(h["time"]),
        "temperature":  h["temperature_2m"],
        "feels_like":   h["apparent_temperature"],
        "precipitation": h["precipitation"],
        "snowfall":     h["snowfall"],
        "windspeed":    h["windspeed_10m"],
        "weathercode":  h["weathercode"],
    })

    df["date"] = df["datetime"].dt.date
    df["hour"] = df["datetime"].dt.hour

    # Fitur turunan
    df["is_rain"] = (df["precipitation"] > 0.1).astype(int)
    df["is_snow"] = (df["snowfall"] > 0.0).astype(int)

    # Isi null (bisa muncul di batas tanggal)
    for col in ["temperature", "feels_like", "precipitation", "snowfall", "windspeed"]:
        df[col] = df[col].fillna(0.0)
    df["weathercode"] = df["weathercode"].fillna(0).astype(int)

    df = df[[
        "date", "hour", "temperature", "feels_like",
        "precipitation", "snowfall", "windspeed",
        "weathercode", "is_rain", "is_snow",
    ]]

    con = duckdb.connect(DB_PATH)
    con.execute("DROP TABLE IF EXISTS dim_weather")
    con.execute("CREATE TABLE dim_weather AS SELECT * FROM df")
    count = con.execute("SELECT COUNT(*) FROM dim_weather").fetchone()[0]
    con.close()

    logger.info(f"dim_weather: {count:,} baris ({df['date'].min()} - {df['date'].max()})")
    return count
