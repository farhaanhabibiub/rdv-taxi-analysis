# 🚕 NYC Taxi Manhattan Analysis Dashboard

Dashboard analisis transportasi NYC berbasis **Yellow Taxi** dan **HVFHV (Uber & Lyft)** di Manhattan, dilengkapi pipeline data engineering modular, integrasi data cuaca eksternal, dan model prediksi permintaan berbasis Machine Learning.

---

## 📊 Project Overview

Project ini menganalisis pola perjalanan taksi di Manhattan menggunakan dataset resmi **NYC TLC Trip Record** periode **Januari – Maret 2026**, ditambah data cuaca dari **Open-Meteo Archive API** sebagai fitur eksternal.

Fitur utama:
- Automated ingestion NYC TLC parquet (Yellow & HVFHV)
- Data cleaning & preprocessing (filter Manhattan zones, validasi tarif/jarak/durasi)
- DuckDB warehouse dengan **star schema** (`fact_trips`, `dim_location`, `dim_operator`, `dim_weather`)
- Tabel agregasi siap-pakai (`agg_hourly_demand`, `agg_zone_demand`)
- Modular ETL pipeline ter-orkestrasi **Prefect**
- Integrasi data cuaca eksternal dari Open-Meteo (suhu, hujan, salju, angin)
- Model prediksi permintaan trip per jam per zona menggunakan **XGBoost**
- Dashboard Streamlit interaktif: peta choropleth Manhattan, time-series, market share, prediksi ML interaktif

---

## 🛠️ Tech Stack

| Kategori | Tools |
|---|---|
| Bahasa | Python 3.10+ |
| Data warehouse | DuckDB |
| Data processing | Pandas, PyArrow |
| Orchestration | Prefect |
| Visualisasi | Streamlit, Plotly, Folium, GeoPandas |
| Machine Learning | XGBoost, scikit-learn, joblib |
| Data eksternal | Open-Meteo Archive API |

---

## 📂 Project Structure

```text
rdv-taxi-analysis/
│
├── dashboard/
│   └── dashboard.py             # Aplikasi Streamlit (peta, chart, prediksi ML)
│
├── pipeline/
│   ├── __init__.py
│   ├── config.py                # Path, URL TLC, zona Manhattan, daftar bulan
│   ├── ingestion.py             # Download parquet & zone lookup
│   ├── cleaning.py              # Preprocessing Yellow & HVFHV (filter + transformasi)
│   ├── transform.py             # Setup schema DuckDB & load ke fact_trips
│   ├── analysis.py              # Bangun tabel agregasi (hourly & zone)
│   ├── weather.py               # Fetch data cuaca Open-Meteo → dim_weather
│   ├── ml_features.py           # Single source of truth feature engineering ML
│   ├── ml_demand.py             # Training XGBoost & simpan model + prediksi
│   └── pipeline.py              # Prefect flow utama (chaining semua task)
│
├── models/
│   └── demand_model.joblib      # Model XGBoost hasil training
│
├── data/
│   ├── raw/                     # Parquet asli dari NYC TLC
│   ├── clean/                   # Parquet hasil preprocessing
│   ├── taxi_zones.geojson       # GeoJSON zona NYC untuk Folium choropleth
│   └── dokumentasi/             # Screenshot hasil pipeline & dashboard
│
├── nyc_taxi_manhattan.duckdb    # Database warehouse (auto-download dari GDrive)
├── taxi_zone_lookup.csv         # Lookup zona NYC TLC
├── cek_database.py              # Script utilitas inspect isi DuckDB
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup Environment

### 1. Clone repository

```bash
git clone <repo-url>
cd "Tugas RDV"
```

### 2. (Opsional, disarankan) Buat virtual environment

```bash
# Windows (PowerShell)
python -m venv .venv
.venv\Scripts\Activate.ps1

# macOS / Linux
python -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

## ▶️ Cara Menjalankan

Ada **dua entry point** terpisah: ETL pipeline (sekali jalan untuk membangun warehouse) dan dashboard Streamlit (untuk eksplorasi).

### A. Jalankan Pipeline ETL (Prefect)

Pipeline akan: download parquet TLC → preprocessing → load ke DuckDB → bangun agregasi → fetch cuaca Open-Meteo → training model XGBoost.

```bash
python -m pipeline.pipeline
```

Default memproses Januari, Februari, Maret 2026 (sesuai `pipeline/config.py:MONTHS`).

Output:
- `nyc_taxi_manhattan.duckdb` — warehouse dengan semua tabel
- `models/demand_model.joblib` — model XGBoost ter-training
- Log Prefect di terminal

### B. Jalankan Dashboard Streamlit

```bash
streamlit run dashboard/dashboard.py
```

Dashboard akan otomatis:
- Mengunduh `nyc_taxi_manhattan.duckdb` dari Google Drive jika belum ada
- Mengunduh `taxi_zones.geojson` dari NYC TLC jika belum ada
- Memuat model XGBoost dari `models/demand_model.joblib`
- Fallback training on-the-fly kalau artifact model belum tersedia

Buka di browser: `http://localhost:8501`

### C. Inspect DuckDB (opsional)

```bash
python cek_database.py
```

---

## 🌤️ Data Eksternal: Open-Meteo

Data cuaca per jam diambil dari **Open-Meteo Archive API** (free, tanpa API key) untuk koordinat Manhattan NYC:

- Koordinat: `40.7128°N, 74.0060°W`
- Timezone: `America/New_York`
- Periode: `2026-01-01` s/d `2026-03-31`
- Variabel: `temperature_2m`, `apparent_temperature`, `precipitation`, `snowfall`, `windspeed_10m`, `weathercode`
- Fitur turunan: `is_rain` (precipitation > 0.1mm), `is_snow` (snowfall > 0cm)

Data disimpan di tabel `dim_weather` dan di-JOIN ke `fact_trips` saat membangun `agg_hourly_demand`. Cuaca dipakai sebagai **fitur tambahan** dalam model demand prediction.

---

## 🤖 Model Machine Learning

**Algoritma:** XGBoost Regressor
**Target:** jumlah trip per jam per zona (`trip_count` di `agg_hourly_demand`)
**Train / Test split:** time-based — Januari-Februari 2026 (train), Maret 2026 (test)

**Fitur (16 total):**
- Waktu: `hour`, `day_of_week`, `month`, `is_weekend`
- Cyclical encoding: `hour_sin`, `hour_cos`, `dow_sin`, `dow_cos`
- Lokasi: `PULocationID`
- Operator: `category_enc`, `operator_enc`
- Cuaca: `temperature`, `precipitation`, `windspeed`, `is_rain`, `is_snow`

**Hyperparameter:**
```text
n_estimators=300, max_depth=6, learning_rate=0.05,
subsample=0.8, colsample_bytree=0.8, tree_method="hist"
```

**Output yang disimpan:**
- `models/demand_model.joblib` — model + feature columns
- Tabel `ml_demand_predictions` — actual vs predicted
- Tabel `ml_feature_importance` — importance per fitur
- Tabel `ml_model_metrics` — RMSE, MAE, R²

Dashboard memuat semua artifact ini langsung (tidak training ulang) sehingga metrik yang ditampilkan konsisten dengan hasil pipeline.

---

## 📐 Skema Warehouse (DuckDB)

| Tabel | Tipe | Deskripsi |
|---|---|---|
| `fact_trips` | Fact | Satu baris per trip Yellow/HVFHV (post-cleaning) |
| `dim_location` | Dimension | Zona Manhattan (LocationID → Zone name, Borough) |
| `dim_operator` | Dimension | Operator (VeriFone, CMT, Uber, Lyft) |
| `dim_weather` | Dimension | Cuaca per jam Manhattan (Open-Meteo) |
| `agg_hourly_demand` | Aggregation | Trip count per jam × zona × operator + cuaca |
| `agg_zone_demand` | Aggregation | Trip count per bulan × zona × operator |
| `ml_demand_predictions` | ML output | Hasil prediksi test set (Maret 2026) |
| `ml_feature_importance` | ML output | Feature importance XGBoost |
| `ml_model_metrics` | ML output | RMSE, MAE, R² |

---

## 👥 Anggota Kelompok

Tugas Proyek Akhir mata kuliah **Rekayasa dan Visualisasi Data** Genap 2025/2026

| NIM | Nama |
|---|---|
| 235150201111005 | Farhaan Habibi |
| 235150201111010 | Muhamad Fa'iz Al Akbar |
| 235150201111011 | Rafly Januar Raharjo |
| 235150201111012 | Arif Rahman |
| 235150207111012 | Yoshia Benedict Parasian |
