# Anggota Kelompok:
# 1. 235150201111005 - Farhaan Habibi
# 2. 235150201111010 - Muhamad Fa'iz Al Akbar
# 3. 235150201111011 - Rafly Januar Raharjo
# 4. 235150201111012 - Arif Rahman
# 5. 235150207111012 - Yoshia Benedict Parasian

"""Single source of truth untuk feature engineering model demand.

Dipakai oleh:
- pipeline/ml_demand.py  (training & simpan model)
- dashboard/dashboard.py (load model + fallback training + prediksi interaktif)
"""

import numpy as np
import pandas as pd


FEATURE_COLS = [
    "hour", "day_of_week", "month", "PULocationID",
    "is_weekend", "category_enc", "operator_enc",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    # Fitur cuaca eksternal (Open-Meteo)
    "temperature", "precipitation", "windspeed", "is_rain", "is_snow",
]

OPERATOR_MAP = {
    "Creative Mobile Tech": 0,
    "VeriFone":             1,
    "Uber":                 2,
    "Lyft":                 3,
}

WEATHER_DEFAULTS = [
    ("temperature",   15.0),
    ("precipitation",  0.0),
    ("windspeed",     10.0),
    ("is_rain",        0),
    ("is_snow",        0),
]


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Hasilkan kolom fitur ML dari agg_hourly_demand.

    Membutuhkan kolom input: date, hour, day_of_week, category, operator_name.
    Kolom cuaca (temperature/precipitation/windspeed/is_rain/is_snow) opsional;
    kalau tidak ada akan diisi default, kalau ada akan di-fillna default.
    """
    df = df.copy()
    df["month"]        = pd.to_datetime(df["date"]).dt.month
    df["is_weekend"]   = df["day_of_week"].isin([0, 6]).astype(int)
    df["hour_sin"]     = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"]     = np.cos(2 * np.pi * df["hour"] / 24)
    df["dow_sin"]      = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["dow_cos"]      = np.cos(2 * np.pi * df["day_of_week"] / 7)
    df["category_enc"] = (df["category"] == "Modern").astype(int)
    df["operator_enc"] = df["operator_name"].map(OPERATOR_MAP).fillna(-1).astype(int)

    for col, default in WEATHER_DEFAULTS:
        if col not in df.columns:
            df[col] = default
        else:
            df[col] = df[col].fillna(default)

    return df
