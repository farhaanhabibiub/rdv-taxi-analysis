# Anggota Kelompok:
# 1. 235150201111005 - Farhaan Habibi
# 2. 235150201111010 - Muhamad Fa'iz Al Akbar
# 3. 235150201111011 - Rafly Januar Raharjo
# 4. 235150201111012 - Arif Rahman
# 5. 235150207111012 - Yoshia Benedict Parasian

import streamlit as st
import duckdb
import pandas as pd
import numpy as np
import folium
from streamlit_folium import st_folium
import plotly.express as px
import plotly.graph_objects as go
import requests
import json
import zipfile
import tempfile
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================

st.set_page_config(
    page_title="NYC Taxi Dashboard",
    page_icon="🚕",
    layout="wide"
)

ROOT_DIR     = Path(__file__).resolve().parent.parent
DB_PATH      = str(ROOT_DIR / "nyc_taxi_manhattan.duckdb")
GEOJSON_PATH = ROOT_DIR / "data" / "taxi_zones.geojson"


GDRIVE_FILE_ID = "1OOwAS8p5x6fOvjaY9mr8o3VoM-0XImn3"

@st.cache_resource(show_spinner="Mengunduh database, mohon tunggu beberapa menit...")
def ensure_database():
    if Path(DB_PATH).exists():
        return True
    import gdown
    url = f"https://drive.google.com/uc?id={GDRIVE_FILE_ID}"
    gdown.download(url, DB_PATH, quiet=False)
    return True

try:
    ensure_database()
except Exception as e:
    st.error(f"Gagal mengunduh database: {e}")
    st.stop()

MONTH_MAP = {
    "Semua Bulan":   None,
    "Januari 2026":  "2026-01-01",
    "Februari 2026": "2026-02-01",
    "Maret 2026":    "2026-03-01",
}

CATEGORY_COLOR = {
    "Konvensional": "#F5A623",
    "Modern":       "#4A90D9",
}

OPERATOR_COLOR = {
    "VeriFone":            "#F5A623",
    "Creative Mobile Tech":"#E8C84D",
    "Uber":                "#252525",
    "Lyft":                "#E84393",
}

DAY_LABEL = {0: "Min", 1: "Sen", 2: "Sel", 3: "Rab", 4: "Kam", 5: "Jum", 6: "Sab"}

# ============================================================
# DATA LOADERS
# ============================================================

@st.cache_resource
def get_con():
    return duckdb.connect(DB_PATH, read_only=True)

@st.cache_data
def load_geojson():
    if GEOJSON_PATH.exists():
        with open(GEOJSON_PATH, encoding="utf-8") as f:
            return json.load(f)
    import geopandas as gpd
    st.info("Mengunduh shapefile zona NYC...")
    r = requests.get(
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip", timeout=60
    )
    GEOJSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        zp = Path(tmp) / "z.zip"
        zp.write_bytes(r.content)
        with zipfile.ZipFile(zp) as z:
            z.extractall(tmp)
        shp = list(Path(tmp).rglob("*.shp"))[0]
        gdf = gpd.read_file(shp).to_crs(epsg=4326)
        txt = gdf.to_json()
        GEOJSON_PATH.write_text(txt, encoding="utf-8")
        return json.loads(txt)

@st.cache_data
def load_zone(month_val, category):
    con = get_con()
    wm = f"AND month = '{month_val}'" if month_val else ""
    wc = f"AND category = '{category}'" if category != "Semua" else ""
    return con.execute(f"""
        SELECT
            PULocationID, zone_name, category, operator_name,
            SUM(trip_count)                AS trip_count,
            ROUND(AVG(avg_fare),       2)  AS avg_fare,
            ROUND(AVG(avg_tip),        2)  AS avg_tip,
            ROUND(AVG(avg_distance),   2)  AS avg_distance,
            ROUND(AVG(avg_duration_min),2) AS avg_duration_min
        FROM agg_zone_demand
        WHERE 1=1 {wm} {wc}
        GROUP BY PULocationID, zone_name, category, operator_name
        ORDER BY trip_count DESC
    """).df()

@st.cache_data
def load_hourly(month_val, category):
    con = get_con()
    wm = f"AND date >= '{month_val}' AND date < (DATE '{month_val}' + INTERVAL 1 MONTH)" if month_val else ""
    wc = f"AND category = '{category}'" if category != "Semua" else ""
    return con.execute(f"""
        SELECT
            hour, day_of_week, category, operator_name,
            SUM(trip_count)              AS trip_count,
            ROUND(AVG(avg_fare),    2)   AS avg_fare,
            ROUND(AVG(avg_tip),     2)   AS avg_tip,
            ROUND(AVG(avg_distance),2)   AS avg_distance,
            ROUND(AVG(avg_duration_min),2) AS avg_duration_min
        FROM agg_hourly_demand
        WHERE 1=1 {wm} {wc}
        GROUP BY hour, day_of_week, category, operator_name
        ORDER BY hour
    """).df()

@st.cache_data
def load_daily(month_val, category):
    con = get_con()
    wm = f"AND date >= '{month_val}' AND date < (DATE '{month_val}' + INTERVAL 1 MONTH)" if month_val else ""
    wc = f"AND category = '{category}'" if category != "Semua" else ""
    return con.execute(f"""
        SELECT
            date, category, operator_name,
            SUM(trip_count)           AS trip_count,
            ROUND(AVG(avg_fare),  2)  AS avg_fare,
            ROUND(AVG(avg_tip),   2)  AS avg_tip
        FROM agg_hourly_demand
        WHERE 1=1 {wm} {wc}
        GROUP BY date, category, operator_name
        ORDER BY date
    """).df()

@st.cache_data
def load_trend_all(category):
    """Selalu load Jan-Mar 2026 untuk tren harian, tidak terpengaruh filter bulan."""
    con = get_con()
    wc = f"AND category = '{category}'" if category != "Semua" else ""
    return con.execute(f"""
        SELECT
            date, category,
            SUM(trip_count) AS trip_count
        FROM agg_hourly_demand
        WHERE date >= '2026-01-01'
          AND date < '2026-04-01'
          {wc}
        GROUP BY date, category
        ORDER BY date
    """).df()

# ============================================================
# ML: Konstanta & helper feature engineering
# ============================================================

ML_FEATURE_COLS = [
    "hour", "day_of_week", "month", "PULocationID",
    "is_weekend", "category_enc", "operator_enc",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    # Fitur cuaca eksternal (Open-Meteo)
    "temperature", "precipitation", "windspeed", "is_rain", "is_snow",
]

ML_FEATURE_LABEL = {
    "hour":          "Jam",
    "day_of_week":   "Hari dalam Minggu",
    "month":         "Bulan",
    "PULocationID":  "ID Zona Pickup",
    "is_weekend":    "Akhir Pekan",
    "category_enc":  "Kategori",
    "operator_enc":  "Operator",
    "hour_sin":      "Jam (sin)",
    "hour_cos":      "Jam (cos)",
    "dow_sin":       "Hari (sin)",
    "dow_cos":       "Hari (cos)",
    "temperature":   "Suhu (°C)",
    "precipitation": "Curah Hujan (mm)",
    "windspeed":     "Kecepatan Angin (km/h)",
    "is_rain":       "Hujan",
    "is_snow":       "Salju",
}

ML_OPERATOR_MAP = {"Creative Mobile Tech": 0, "VeriFone": 1, "Uber": 2, "Lyft": 3}

ML_OPERATOR_BY_CAT = {
    "Konvensional": ["VeriFone", "Creative Mobile Tech"],
    "Modern":       ["Uber", "Lyft"],
}

ML_DAY_OPTIONS = ["Minggu", "Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu"]


def _build_ml_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["month"]        = pd.to_datetime(df["date"]).dt.month
    df["is_weekend"]   = df["day_of_week"].isin([0, 6]).astype(int)
    df["hour_sin"]     = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"]     = np.cos(2 * np.pi * df["hour"] / 24)
    df["dow_sin"]      = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["dow_cos"]      = np.cos(2 * np.pi * df["day_of_week"] / 7)
    df["category_enc"] = (df["category"] == "Modern").astype(int)
    df["operator_enc"] = df["operator_name"].map(ML_OPERATOR_MAP).fillna(-1).astype(int)
    return df


@st.cache_resource(show_spinner="Melatih model XGBoost, mohon tunggu...")
def get_ml_model():
    from xgboost import XGBRegressor
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

    con = get_con()

    # Coba load dengan data cuaca; fallback ke default jika dim_weather belum ada
    try:
        con.execute("SELECT 1 FROM dim_weather LIMIT 1")
        df = con.execute("""
            SELECT h.date, h.hour, h.day_of_week, h.PULocationID,
                   h.category, h.operator_name, h.trip_count,
                   COALESCE(w.temperature,   15.0) AS temperature,
                   COALESCE(w.precipitation,  0.0) AS precipitation,
                   COALESCE(w.windspeed,      10.0) AS windspeed,
                   COALESCE(w.is_rain,        0)    AS is_rain,
                   COALESCE(w.is_snow,        0)    AS is_snow
            FROM agg_hourly_demand h
            LEFT JOIN dim_weather w ON h.date = w.date AND h.hour = w.hour
            ORDER BY h.date, h.hour, h.PULocationID
        """).df()
    except Exception:
        df = con.execute("""
            SELECT date, hour, day_of_week, PULocationID,
                   category, operator_name, trip_count
            FROM agg_hourly_demand
            ORDER BY date, hour, PULocationID
        """).df()
        for col, val in [("temperature", 15.0), ("precipitation", 0.0),
                          ("windspeed", 10.0), ("is_rain", 0), ("is_snow", 0)]:
            df[col] = val

    df = _build_ml_features(df)

    train = df[pd.to_datetime(df["date"]) < "2026-03-01"].copy()
    test  = df[pd.to_datetime(df["date"]) >= "2026-03-01"].copy()

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
    model.fit(train[ML_FEATURE_COLS], train["trip_count"])

    y_pred = model.predict(test[ML_FEATURE_COLS]).clip(0)
    y_test = test["trip_count"].values

    metrics = {
        "rmse": float(np.sqrt(mean_squared_error(y_test, y_pred))),
        "mae":  float(mean_absolute_error(y_test, y_pred)),
        "r2":   float(r2_score(y_test, y_pred)),
    }

    pred_df = test[["date", "hour", "day_of_week", "PULocationID",
                     "category", "operator_name", "trip_count"]].copy()
    pred_df["predicted"] = y_pred.round(1).astype(float)

    fi_df = pd.DataFrame({
        "feature":    [ML_FEATURE_LABEL[f] for f in ML_FEATURE_COLS],
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=True)

    return model, metrics, pred_df, fi_df


@st.cache_data
def load_zone_list():
    con = get_con()
    return con.execute("""
        SELECT DISTINCT PULocationID, zone_name
        FROM agg_zone_demand
        ORDER BY zone_name
    """).df()


@st.cache_data
def load_weather_daily():
    con = get_con()
    try:
        con.execute("SELECT 1 FROM dim_weather LIMIT 1")
    except Exception:
        return pd.DataFrame()
    return con.execute("""
        SELECT
            date,
            ROUND(AVG(temperature),  1) AS avg_temp,
            ROUND(MAX(temperature),  1) AS max_temp,
            ROUND(MIN(temperature),  1) AS min_temp,
            ROUND(SUM(precipitation),1) AS total_precip,
            ROUND(MAX(snowfall),     1) AS max_snowfall,
            MAX(is_rain)               AS had_rain,
            MAX(is_snow)               AS had_snow
        FROM dim_weather
        GROUP BY date
        ORDER BY date
    """).df()


# ============================================================
# HELPER: PETA
# ============================================================

def build_map(df_zone, geojson, title, color="YlOrRd"):
    m = folium.Map(
        location=[40.754, -73.984],
        zoom_start=13,
        tiles="CartoDB positron"
    )
    if df_zone.empty:
        return m

    folium.Choropleth(
        geo_data=geojson,
        data=df_zone,
        columns=["PULocationID", "trip_count"],
        key_on="feature.properties.LocationID",
        fill_color=color,
        fill_opacity=0.75,
        line_opacity=0.3,
        legend_name=f"Jumlah Trip - {title}",
        nan_fill_color="lightgray",
    ).add_to(m)

    tip_map = df_zone.set_index("PULocationID")[
        ["zone_name", "trip_count", "avg_fare", "avg_distance"]
    ].to_dict("index")

    for feat in geojson["features"]:
        lid = feat["properties"].get("LocationID")
        if lid in tip_map:
            d = tip_map[lid]
            folium.GeoJson(
                feat,
                style_function=lambda x: {"fillOpacity": 0, "weight": 0},
                tooltip=folium.Tooltip(
                    f"<b>{d['zone_name']}</b><br>"
                    f"Trip: {int(d['trip_count']):,}<br>"
                    f"Avg Tarif: ${d['avg_fare']:.2f}<br>"
                    f"Avg Jarak: {d['avg_distance']:.2f} mil"
                )
            ).add_to(m)
    return m

# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:
    st.title("Filter Dashboard")
    st.markdown("---")

    bulan = st.selectbox("Bulan", options=list(MONTH_MAP.keys()), index=0)
    month_val = MONTH_MAP[bulan]

    kategori = st.selectbox("Kategori", options=["Semua", "Konvensional", "Modern"], index=0)

    st.markdown("---")
    st.caption("Sumber: NYC TLC Trip Record")
    st.caption("Wilayah: Manhattan Zone")
    st.caption("Periode: Januari - Maret 2026")

# ============================================================
# LOAD DATA
# ============================================================

df_zone   = load_zone(month_val, kategori)
df_hourly = load_hourly(month_val, kategori)
df_daily  = load_daily(month_val, kategori)
geojson   = load_geojson()

# ============================================================
# HEADER
# ============================================================

st.title("NYC Taxi Dashboard - Manhattan Zone")
st.markdown(
    f"**Analisis Perbandingan Antara Yellow Taxi dan HV-FHV di Manhattan Berdasarkan Lokasi dan Waktu (Jan-Mar 2026)** &nbsp;|&nbsp; "
    f"Filter: **{bulan}** &nbsp;|&nbsp; Kategori: **{kategori}**"
)
st.markdown("---")

# ============================================================
# KPI (Tarif dan Tips)
# ============================================================

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Trip",         f"{df_zone['trip_count'].sum():,.0f}")
k2.metric("Rata-rata Tarif",    f"${df_zone['avg_fare'].mean():.2f}")
k3.metric("Rata-rata Tips",     f"${df_zone['avg_tip'].mean():.2f}")
k4.metric("Rata-rata Jarak",    f"{df_zone['avg_distance'].mean():.2f} mil")
k5.metric("Zona Aktif",         f"{df_zone['PULocationID'].nunique()} zona")

st.markdown("---")

# ============================================================
# SECTION 1 - PETA
# ============================================================

st.header("🗺️ Peta Intensitas Pickup - Manhattan Zone")

df_yellow = df_zone[df_zone["category"] == "Konvensional"].groupby(
    ["PULocationID", "zone_name"], as_index=False
).agg({"trip_count": "sum", "avg_fare": "mean", "avg_distance": "mean"})

df_fhvhv = df_zone[df_zone["category"] == "Modern"].groupby(
    ["PULocationID", "zone_name"], as_index=False
).agg({"trip_count": "sum", "avg_fare": "mean", "avg_distance": "mean"})

mc1, mc2 = st.columns(2)
with mc1:
    st.subheader("🟡 Yellow Taxi (Konvensional)")
    m1 = build_map(df_yellow, geojson, "Yellow Taxi", "YlOrRd")
    st_folium(m1, width=520, height=430, key="map_yellow")

with mc2:
    st.subheader("🔵 HVFHV - Uber & Lyft (Modern)")
    m2 = build_map(df_fhvhv, geojson, "HVFHV", "Blues")
    st_folium(m2, width=520, height=430, key="map_fhvhv")

st.markdown("---")

# ============================================================
# SECTION 2 - TOP 10 ZONA
# ============================================================

st.header("📍 Top 10 Zona Terpopuler")

z1, z2 = st.columns(2)

with z1:
    df_top_yellow = df_zone[df_zone["category"] == "Konvensional"].groupby(
        "zone_name", as_index=False
    ).agg({"trip_count": "sum"}).sort_values("trip_count", ascending=False).head(10)

    fig_zy = px.bar(
        df_top_yellow,
        x="trip_count", y="zone_name",
        orientation="h",
        color="trip_count",
        color_continuous_scale="YlOrRd",
        title="Top 10 Zona Pickup - Yellow Taxi",
        labels={"trip_count": "Jumlah Trip", "zone_name": "Zona"}
    )
    fig_zy.update_layout(
        yaxis=dict(autorange="reversed"),
        showlegend=False,
        height=400,
        coloraxis_showscale=False
    )
    st.plotly_chart(fig_zy, use_container_width=True)

with z2:
    df_top_fhvhv = df_zone[df_zone["category"] == "Modern"].groupby(
        "zone_name", as_index=False
    ).agg({"trip_count": "sum"}).sort_values("trip_count", ascending=False).head(10)

    fig_zf = px.bar(
        df_top_fhvhv,
        x="trip_count", y="zone_name",
        orientation="h",
        color="trip_count",
        color_continuous_scale="Blues",
        title="Top 10 Zona Pickup - HVFHV (Uber & Lyft)",
        labels={"trip_count": "Jumlah Trip", "zone_name": "Zona"}
    )
    fig_zf.update_layout(
        yaxis=dict(autorange="reversed"),
        showlegend=False,
        height=400,
        coloraxis_showscale=False
    )
    st.plotly_chart(fig_zf, use_container_width=True)

st.markdown("---")

# ============================================================
# SECTION 3 - MARKET SHARE
# ============================================================

st.header("📊 Market Share & Volume Trip")

s1, s2 = st.columns(2)

with s1:
    # Pie chart market share per kategori
    df_share = df_zone.groupby("category", as_index=False).agg({"trip_count": "sum"})
    fig_pie = px.pie(
        df_share,
        values="trip_count",
        names="category",
        color="category",
        color_discrete_map=CATEGORY_COLOR,
        title="Market Share: Konvensional vs Modern",
        hole=0.45
    )
    fig_pie.update_traces(textinfo="percent+label", textfont_size=13)
    fig_pie.update_layout(showlegend=False, height=380)
    st.plotly_chart(fig_pie, use_container_width=True)

with s2:
    # Bar chart volume per operator
    df_op = df_zone.groupby("operator_name", as_index=False).agg(
        {"trip_count": "sum"}
    ).sort_values("trip_count", ascending=False)
    fig_op = px.bar(
        df_op,
        x="operator_name", y="trip_count",
        color="operator_name",
        color_discrete_map=OPERATOR_COLOR,
        title="Total Trip per Operator",
        labels={"operator_name": "Operator", "trip_count": "Jumlah Trip"},
        text="trip_count"
    )
    fig_op.update_traces(
        texttemplate="%{text:,.0f}", textposition="outside"
    )
    fig_op.update_layout(showlegend=False, height=380)
    st.plotly_chart(fig_op, use_container_width=True)

st.markdown("---")

# ============================================================
# SECTION 4 - POLA WAKTU
# ============================================================

st.header("⏰ Pola Waktu - Volume Trip")

tab_line, tab_heat = st.tabs(["Volume Trip (Line Chart)", "Heatmap"])

with tab_line:
    df_line = df_hourly.groupby(["hour", "category"], as_index=False).agg(
        {"trip_count": "sum"}
    )
    fig_line = px.line(
        df_line,
        x="hour", y="trip_count",
        color="category",
        color_discrete_map=CATEGORY_COLOR,
        markers=True,
        title="Volume Trip per Jam",
        labels={"hour": "Jam", "trip_count": "Jumlah Trip", "category": "Kategori"}
    )
    fig_line.update_layout(
        xaxis=dict(tickmode="linear", dtick=1, title="Jam (0-23)"),
        height=400
    )
    st.plotly_chart(fig_line, use_container_width=True)

with tab_heat:
    df_heat = df_hourly.groupby(["hour", "day_of_week"], as_index=False).agg(
        {"trip_count": "sum"}
    )
    df_heat["hari"] = df_heat["day_of_week"].map(DAY_LABEL)
    df_pivot = df_heat.pivot(index="hari", columns="hour", values="trip_count")
    df_pivot = df_pivot.reindex(["Sen", "Sel", "Rab", "Kam", "Jum", "Sab", "Min"])
    fig_heat = px.imshow(
        df_pivot,
        color_continuous_scale="YlOrRd",
        title="Heatmap Volume Trip - Jam × Hari",
        labels=dict(x="Jam", y="Hari", color="Jumlah Trip"),
        aspect="auto"
    )
    fig_heat.update_layout(height=350)
    st.plotly_chart(fig_heat, use_container_width=True)

st.markdown("---")

# ============================================================
# SECTION 5 - TREN HARIAN
# ============================================================

st.header("📅 Tren Harian - Januari s/d Maret 2026")

df_trend = load_trend_all(kategori)

# Garis vertikal batas bulan
bulan_batas = ["2026-02-01", "2026-03-01"]

fig_trend = px.line(
    df_trend,
    x="date", y="trip_count",
    color="category",
    color_discrete_map=CATEGORY_COLOR,
    title="Volume Trip Harian - Januari s/d Maret 2026",
    labels={"date": "Tanggal", "trip_count": "Jumlah Trip", "category": "Kategori"}
)

# Garis pemisah bulan
for batas in bulan_batas:
    fig_trend.add_vline(
        x=batas,
        line_dash="dash",
        line_color="gray",
        opacity=0.5
    )

# Label bulan
for label, xpos in [("Januari", "2026-01-15"), ("Februari", "2026-02-14"), ("Maret", "2026-03-15")]:
    fig_trend.add_annotation(
        x=xpos, y=1, yref="paper",
        text=label, showarrow=False,
        font=dict(size=11, color="gray"),
        yanchor="bottom"
    )

fig_trend.update_layout(
    height=400,
    xaxis=dict(
        tickformat="%d %b",
        dtick="M1",
        ticklabelmode="period"
    )
)
st.plotly_chart(fig_trend, use_container_width=True)

st.markdown("---")

# ============================================================
# SECTION 6 - ANALISIS TARIF
# ============================================================

st.header("💰 Analisis Tarif & Tips")

t1, t2, t3 = st.columns(3)

with t1:
    # Rata-rata tarif per operator
    df_fare = df_zone.groupby("operator_name", as_index=False).agg(
        {"avg_fare": "mean"}
    ).sort_values("avg_fare", ascending=False)
    fig_fare = px.bar(
        df_fare,
        x="operator_name", y="avg_fare",
        color="operator_name",
        color_discrete_map=OPERATOR_COLOR,
        title="Rata-rata Tarif per Operator",
        labels={"operator_name": "Operator", "avg_fare": "Tarif ($)"},
        text="avg_fare"
    )
    fig_fare.update_traces(texttemplate="$%{text:.2f}", textposition="outside")
    fig_fare.update_layout(showlegend=False, height=380)
    st.plotly_chart(fig_fare, use_container_width=True)

with t2:
    # Rata-rata tips per operator
    df_tip = df_zone.groupby("operator_name", as_index=False).agg(
        {"avg_tip": "mean"}
    ).sort_values("avg_tip", ascending=False)
    fig_tip = px.bar(
        df_tip,
        x="operator_name", y="avg_tip",
        color="operator_name",
        color_discrete_map=OPERATOR_COLOR,
        title="Rata-rata Tips per Operator",
        labels={"operator_name": "Operator", "avg_tip": "Tips ($)"},
        text="avg_tip"
    )
    fig_tip.update_traces(texttemplate="$%{text:.2f}", textposition="outside")
    fig_tip.update_layout(showlegend=False, height=380)
    st.plotly_chart(fig_tip, use_container_width=True)

with t3:
    # Rata-rata jarak per operator
    df_dist = df_zone.groupby("operator_name", as_index=False).agg(
        {"avg_distance": "mean"}
    ).sort_values("avg_distance", ascending=False)
    fig_dist = px.bar(
        df_dist,
        x="operator_name", y="avg_distance",
        color="operator_name",
        color_discrete_map=OPERATOR_COLOR,
        title="Rata-rata Jarak per Operator",
        labels={"operator_name": "Operator", "avg_distance": "Jarak (mil)"},
        text="avg_distance"
    )
    fig_dist.update_traces(texttemplate="%{text:.2f} mil", textposition="outside")
    fig_dist.update_layout(showlegend=False, height=380)
    st.plotly_chart(fig_dist, use_container_width=True)

st.markdown("---")

# ============================================================
# SECTION 7 - TARIF PER JAM
# ============================================================

st.header("💵 Rata-rata Tarif per Jam")

df_fare_hour = df_hourly.groupby(["hour", "category"], as_index=False).agg(
    {"avg_fare": "mean"}
)
fig_fare_hour = px.line(
    df_fare_hour,
    x="hour", y="avg_fare",
    color="category",
    color_discrete_map=CATEGORY_COLOR,
    markers=True,
    title="Rata-rata Tarif per Jam - Konvensional vs Modern",
    labels={"hour": "Jam", "avg_fare": "Rata-rata Tarif ($)", "category": "Kategori"}
)
fig_fare_hour.update_layout(
    xaxis=dict(tickmode="linear", dtick=1),
    height=380
)
st.plotly_chart(fig_fare_hour, use_container_width=True)

st.markdown("---")

# ============================================================
# SECTION 8 - KONDISI CUACA NYC (DATA EKSTERNAL OPEN-METEO)
# ============================================================

st.header("🌤️ Kondisi Cuaca NYC — Januari s/d Maret 2026")
st.markdown(
    "Data cuaca per jam dari **Open-Meteo Archive API** "
    "(koordinat Manhattan: 40.7128°N, 74.0060°W, timezone: America/New_York). "
    "Digunakan sebagai fitur tambahan pada model prediksi."
)

df_weather = load_weather_daily()

if df_weather.empty:
    st.info("Data cuaca belum tersedia. Jalankan pipeline terlebih dahulu untuk mengunduh data dari Open-Meteo.")
else:
    df_weather["date"] = pd.to_datetime(df_weather["date"])

    # ── KPI cuaca ────────────────────────────────────────────
    wk1, wk2, wk3, wk4 = st.columns(4)
    wk1.metric("Rata-rata Suhu",    f"{df_weather['avg_temp'].mean():.1f} °C")
    wk2.metric("Suhu Terendah",     f"{df_weather['min_temp'].min():.1f} °C")
    wk3.metric("Hari Hujan",        f"{df_weather['had_rain'].sum()} hari")
    wk4.metric("Hari Bersalju",     f"{df_weather['had_snow'].sum()} hari")

    wc1, wc2 = st.columns(2)

    with wc1:
        # Grafik suhu harian
        fig_temp = go.Figure()
        fig_temp.add_trace(go.Scatter(
            x=df_weather["date"], y=df_weather["max_temp"],
            name="Suhu Maks", line=dict(color="#E74C3C", width=1.5, dash="dot"),
        ))
        fig_temp.add_trace(go.Scatter(
            x=df_weather["date"], y=df_weather["avg_temp"],
            name="Suhu Rata-rata", line=dict(color="#F5A623", width=2),
            fill="tonexty", fillcolor="rgba(245,166,35,0.08)",
        ))
        fig_temp.add_trace(go.Scatter(
            x=df_weather["date"], y=df_weather["min_temp"],
            name="Suhu Min", line=dict(color="#4A90D9", width=1.5, dash="dot"),
            fill="tonexty", fillcolor="rgba(74,144,217,0.08)",
        ))
        # Garis pemisah bulan
        for batas in ["2026-02-01", "2026-03-01"]:
            fig_temp.add_vline(x=batas, line_dash="dash", line_color="gray", opacity=0.4)
        fig_temp.update_layout(
            title="Suhu Harian — Manhattan NYC",
            xaxis_title="Tanggal", yaxis_title="Suhu (°C)",
            xaxis_tickformat="%d %b", height=380, legend_orientation="h",
        )
        st.plotly_chart(fig_temp, use_container_width=True)

    with wc2:
        # Grafik curah hujan + salju harian
        fig_rain = go.Figure()
        fig_rain.add_trace(go.Bar(
            x=df_weather["date"], y=df_weather["total_precip"],
            name="Curah Hujan (mm)", marker_color="#4A90D9", opacity=0.85,
        ))
        fig_rain.add_trace(go.Bar(
            x=df_weather["date"], y=df_weather["max_snowfall"],
            name="Salju Maks (cm)", marker_color="#A8D8EA", opacity=0.85,
        ))
        for batas in ["2026-02-01", "2026-03-01"]:
            fig_rain.add_vline(x=batas, line_dash="dash", line_color="gray", opacity=0.4)
        fig_rain.update_layout(
            title="Curah Hujan & Salju Harian — Manhattan NYC",
            xaxis_title="Tanggal", yaxis_title="Jumlah (mm / cm)",
            xaxis_tickformat="%d %b", barmode="overlay",
            height=380, legend_orientation="h",
        )
        st.plotly_chart(fig_rain, use_container_width=True)

st.markdown("---")

# ============================================================
# SECTION 9 - PREDIKSI PERMINTAAN TRIP (MACHINE LEARNING)
# ============================================================

st.header("🤖 Prediksi Permintaan Trip — XGBoost")

st.markdown(
    "Model **XGBoost** dilatih menggunakan data **Januari–Februari 2026** "
    "dan diuji pada **Maret 2026**. &nbsp;|&nbsp; "
    "**Fitur:** jam, hari, bulan, zona, kategori, operator, fitur siklik (sin/cos jam & hari). &nbsp;|&nbsp; "
    "**Target:** jumlah trip per jam per zona."
)

model_obj, ml_metrics, ml_pred_df, ml_fi_df = get_ml_model()

# ── KPI Metrics ──────────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)
m1.metric("Model", "XGBoost")
m2.metric("RMSE", f"{ml_metrics['rmse']:.2f} trip")
m3.metric("MAE",  f"{ml_metrics['mae']:.2f} trip")
m4.metric("R² Score", f"{ml_metrics['r2']:.4f}")

tab_perf, tab_pred = st.tabs(["📊 Performa Model", "🔮 Prediksi Interaktif"])

# ── Tab 1: Performa Model ─────────────────────────────────────
with tab_perf:
    p1, p2 = st.columns(2)

    with p1:
        # Actual vs Predicted — agregasi per tanggal & kategori
        comp_df = ml_pred_df.groupby(["date", "category"], as_index=False).agg(
            Aktual=("trip_count", "sum"),
            Prediksi=("predicted", "sum"),
        )
        comp_melt = comp_df.melt(
            id_vars=["date", "category"],
            value_vars=["Aktual", "Prediksi"],
            var_name="Tipe", value_name="trip_count",
        )
        fig_comp = px.line(
            comp_melt,
            x="date", y="trip_count",
            color="category",
            line_dash="Tipe",
            color_discrete_map=CATEGORY_COLOR,
            title="Aktual vs Prediksi — Maret 2026",
            labels={"date": "Tanggal", "trip_count": "Jumlah Trip", "category": "Kategori"},
        )
        fig_comp.update_layout(height=400, xaxis_tickformat="%d %b")
        st.plotly_chart(fig_comp, use_container_width=True)

    with p2:
        fig_fi = px.bar(
            ml_fi_df,
            x="importance", y="feature",
            orientation="h",
            title="Feature Importance — XGBoost",
            labels={"importance": "Importance", "feature": "Fitur"},
            color="importance",
            color_continuous_scale="Blues",
        )
        fig_fi.update_layout(
            height=400,
            showlegend=False,
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig_fi, use_container_width=True)

# ── Tab 2: Prediksi Interaktif ────────────────────────────────
with tab_pred:
    st.subheader("Prediksi Jumlah Trip per Jam & Zona")
    st.caption("Masukkan parameter di bawah untuk melihat prediksi demand dari model XGBoost.")

    zone_df     = load_zone_list()
    zone_lookup = zone_df.set_index("zone_name")["PULocationID"].to_dict()

    i1, i2, i3, i4, i5, i6 = st.columns(6)
    sel_zona    = i1.selectbox("Zona",     options=list(zone_lookup.keys()), key="pred_zona")
    sel_hari    = i2.selectbox("Hari",     options=ML_DAY_OPTIONS, index=1,  key="pred_hari")
    sel_bulan   = i3.selectbox("Bulan",    options=["Januari", "Februari", "Maret"],
                               index=0, key="pred_bulan")
    sel_jam     = i4.slider("Jam", 0, 23, 8, key="pred_jam")
    sel_kat     = i5.selectbox("Kategori", options=["Konvensional", "Modern"],  key="pred_kat")
    sel_op      = i6.selectbox("Operator", options=ML_OPERATOR_BY_CAT[sel_kat], key="pred_op")

    st.markdown("**Kondisi Cuaca (Opsional)**")
    w1, w2, w3 = st.columns(3)
    sel_temp   = w1.slider("Suhu (°C)",          -15.0, 40.0, 10.0, 0.5, key="pred_temp")
    sel_precip = w2.slider("Curah Hujan (mm)",     0.0, 30.0,  0.0, 0.1, key="pred_precip")
    sel_wind   = w3.slider("Kecepatan Angin (km/h)", 0.0, 60.0, 10.0, 0.5, key="pred_wind")
    sel_is_rain = int(sel_precip > 0.1)
    sel_is_snow = int(sel_temp <= 2.0 and sel_precip > 0.0)

    sel_loc_id  = zone_lookup[sel_zona]
    sel_dow     = ML_DAY_OPTIONS.index(sel_hari)
    sel_month   = ["Januari", "Februari", "Maret"].index(sel_bulan) + 1

    # Prediksi satu titik waktu
    def _make_feat(hour, dow, month, loc_id, kategori, operator,
                   temp, precip, wind, is_rain, is_snow):
        return {
            "hour":          hour,
            "day_of_week":   dow,
            "month":         month,
            "PULocationID":  loc_id,
            "is_weekend":    int(dow in [0, 6]),
            "category_enc":  int(kategori == "Modern"),
            "operator_enc":  ML_OPERATOR_MAP.get(operator, -1),
            "hour_sin":      float(np.sin(2 * np.pi * hour / 24)),
            "hour_cos":      float(np.cos(2 * np.pi * hour / 24)),
            "dow_sin":       float(np.sin(2 * np.pi * dow / 7)),
            "dow_cos":       float(np.cos(2 * np.pi * dow / 7)),
            "temperature":   temp,
            "precipitation": precip,
            "windspeed":     wind,
            "is_rain":       is_rain,
            "is_snow":       is_snow,
        }

    single_feat = pd.DataFrame([_make_feat(
        sel_jam, sel_dow, sel_month, sel_loc_id, sel_kat, sel_op,
        sel_temp, sel_precip, sel_wind, sel_is_rain, sel_is_snow,
    )])[ML_FEATURE_COLS]
    single_pred = max(0.0, float(model_obj.predict(single_feat)[0]))

    # Prediksi seluruh 24 jam untuk grafik
    hourly_rows = [_make_feat(h, sel_dow, sel_month, sel_loc_id, sel_kat, sel_op,
                               sel_temp, sel_precip, sel_wind, sel_is_rain, sel_is_snow)
                   for h in range(24)]
    hourly_feat = pd.DataFrame(hourly_rows)[ML_FEATURE_COLS]
    hourly_preds = model_obj.predict(hourly_feat).clip(0).round(1)

    st.markdown("---")
    r1, r2, r3 = st.columns([1.5, 1.5, 3])
    r1.metric("Zona", sel_zona)
    r2.metric(
        f"Prediksi Trip — {sel_hari}, {sel_bulan}, Jam {sel_jam:02d}:00",
        f"{single_pred:,.0f} trip",
    )

    hourly_chart_df = pd.DataFrame({"Jam": list(range(24)), "Prediksi Trip": hourly_preds})
    fig_hourly = px.bar(
        hourly_chart_df,
        x="Jam", y="Prediksi Trip",
        title=f"Prediksi per Jam — {sel_zona} | {sel_hari}, {sel_bulan} | {sel_kat} ({sel_op})",
        color="Prediksi Trip",
        color_continuous_scale="Blues",
        labels={"Jam": "Jam (0-23)", "Prediksi Trip": "Prediksi Jumlah Trip"},
    )
    fig_hourly.add_vline(
        x=sel_jam, line_dash="dash", line_color="red", opacity=0.8,
        annotation_text=f"  Jam {sel_jam:02d}:00", annotation_position="top right"
    )
    fig_hourly.update_layout(
        xaxis=dict(tickmode="linear", dtick=1),
        height=380,
        coloraxis_showscale=False,
    )
    with r3:
        st.plotly_chart(fig_hourly, use_container_width=True)
