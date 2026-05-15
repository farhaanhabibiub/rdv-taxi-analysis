# 🚕 NYC Taxi Manhattan Analysis Dashboard

Dashboard analisis transportasi NYC berbasis Yellow Taxi dan HVFHV menggunakan pipeline data engineering modular dan visualisasi interaktif Streamlit.

---

## 📊 Project Overview

Project ini menganalisis pola perjalanan taksi di Manhattan menggunakan dataset resmi NYC TLC periode Januari–Maret 2026.

Fitur utama:
- Automated ingestion NYC TLC parquet
- Data cleaning & preprocessing
- DuckDB warehouse
- Modular ETL pipeline
- Interactive Streamlit dashboard
- Spatial analysis Manhattan pickup zones

---

## 🛠️ Tech Stack

- Python
- DuckDB
- Pandas
- Prefect
- Streamlit
- Plotly
- GeoPandas
- Folium

---

## 📂 Project Structure

```text
rdv-taxi-analysis/
│
├── dashboard/
│   └── dashboard.py
│
├── data/
│   ├── raw/
│   ├── clean/
│   └── taxi_zones/
│
├── pipeline/
│   ├── ingestion.py
│   ├── cleaning.py
│   ├── transform.py
│   ├── analysis.py
│   ├── pipeline.py
│   └── config.py
│
├── requirements.txt
├── cek_database.py
└── README.md
