# Anggota Kelompok:
# 1. 235150201111005 - Farhaan Habibi
# 2. 235150201111010 - Muhamad Fa'iz Al Akbar
# 3. 235150201111011 - Rafly Januar Raharjo
# 4. 235150201111012 - Arif Rahman
# 5. 235150207111012 - Yoshia Benedict Parasian

import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
import joblib
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from prefect import task, get_run_logger

from pipeline.config import DB_PATH

MODEL_DIR  = Path(__file__).resolve().parent.parent / "models"
MODEL_PATH = MODEL_DIR / "demand_model.joblib"

FEATURE_COLS = [
    "hour", "day_of_week", "month", "PULocationID",
    "is_weekend", "category_enc", "operator_enc",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    # Fitur cuaca eksternal (Open-Meteo)
    "temperature", "precipitation", "windspeed", "is_rain", "is_snow",
]

OPERATOR_MAP = {"Creative Mobile Tech": 0, "VeriFone": 1, "Uber": 2, "Lyft": 3}


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["month"]        = pd.to_datetime(df["date"]).dt.month
    df["is_weekend"]   = df["day_of_week"].isin([0, 6]).astype(int)
    df["hour_sin"]     = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"]     = np.cos(2 * np.pi * df["hour"] / 24)
    df["dow_sin"]      = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["dow_cos"]      = np.cos(2 * np.pi * df["day_of_week"] / 7)
    df["category_enc"] = (df["category"] == "Modern").astype(int)
    df["operator_enc"] = df["operator_name"].map(OPERATOR_MAP).fillna(-1).astype(int)
    # Cuaca — sudah ada dari agg_hourly_demand, isi default jika null
    for col, default in [("temperature", 15.0), ("precipitation", 0.0),
                          ("windspeed", 10.0), ("is_rain", 0), ("is_snow", 0)]:
        if col not in df.columns:
            df[col] = default
        else:
            df[col] = df[col].fillna(default)
    return df


@task(name="Training Model Prediksi Demand")
def train_demand_model() -> dict:
    logger = get_run_logger()
    con = duckdb.connect(DB_PATH)

    df = con.execute("""
        SELECT date, hour, day_of_week, PULocationID,
               category, operator_name, trip_count,
               temperature, precipitation, windspeed, is_rain, is_snow
        FROM agg_hourly_demand
        ORDER BY date, hour, PULocationID
    """).df()

    df = build_features(df)

    # Train: Januari + Februari | Test: Maret
    train = df[pd.to_datetime(df["date"]) < "2026-03-01"].copy()
    test  = df[pd.to_datetime(df["date"]) >= "2026-03-01"].copy()

    X_train, y_train = train[FEATURE_COLS], train["trip_count"]
    X_test,  y_test  = test[FEATURE_COLS],  test["trip_count"]

    logger.info(f"Train: {len(train):,} baris | Test: {len(test):,} baris")

    model = XGBRegressor(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1,
        tree_method="hist",
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    y_pred = model.predict(X_test).clip(0)

    rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
    mae  = float(mean_absolute_error(y_test, y_pred))
    r2   = float(r2_score(y_test, y_pred))

    logger.info(f"RMSE={rmse:.2f}  MAE={mae:.2f}  R²={r2:.4f}")

    # ── simpan predictions ke DuckDB ─────────────────────────
    pred_df = test[["date", "hour", "day_of_week", "PULocationID",
                     "category", "operator_name", "trip_count"]].copy()
    pred_df["predicted"] = y_pred.round(1).astype(float)

    con.execute("DROP TABLE IF EXISTS ml_demand_predictions")
    con.execute("CREATE TABLE ml_demand_predictions AS SELECT * FROM pred_df")

    # ── feature importance ────────────────────────────────────
    fi_df = pd.DataFrame({
        "feature":    FEATURE_COLS,
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=False)

    con.execute("DROP TABLE IF EXISTS ml_feature_importance")
    con.execute("CREATE TABLE ml_feature_importance AS SELECT * FROM fi_df")

    # ── metrics ───────────────────────────────────────────────
    metrics_df = pd.DataFrame([{
        "model_name": "XGBoost",
        "rmse":       round(rmse, 4),
        "mae":        round(mae,  4),
        "r2":         round(r2,   4),
        "train_rows": int(len(train)),
        "test_rows":  int(len(test)),
    }])
    con.execute("DROP TABLE IF EXISTS ml_model_metrics")
    con.execute("CREATE TABLE ml_model_metrics AS SELECT * FROM metrics_df")

    con.close()

    # ── simpan model lokal ────────────────────────────────────
    MODEL_DIR.mkdir(exist_ok=True)
    joblib.dump({"model": model, "feature_cols": FEATURE_COLS}, MODEL_PATH)
    logger.info(f"Model disimpan: {MODEL_PATH}")

    return {"rmse": rmse, "mae": mae, "r2": r2}
