import streamlit as st
import duckdb
import pandas as pd
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

# Google Drive file ID untuk nyc_taxi_manhattan.duckdb
# Ganti GDRIVE_FILE_ID dengan ID file setelah upload ke Google Drive
GDRIVE_FILE_ID = "1ZkBh2s2WD_gF0dmSplBbWBCFEuImMkt6"

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
    "Uber":                "#1C1C1C",
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
        legend_name=f"Jumlah Trip — {title}",
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
    st.title("🚕 Filter Dashboard")
    st.markdown("---")

    bulan = st.selectbox("Bulan", options=list(MONTH_MAP.keys()), index=0)
    month_val = MONTH_MAP[bulan]

    kategori = st.selectbox("Kategori", options=["Semua", "Konvensional", "Modern"], index=0)

    st.markdown("---")
    st.caption("Sumber: NYC TLC Trip Record")
    st.caption("Wilayah: Manhattan Yellow Zone")
    st.caption("Periode: Januari – Maret 2026")


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

st.title("NYC Taxi Dashboard — Manhattan Yellow Zone")
st.markdown(
    f"**Analisis Spasial & Temporal Yellow Taxi vs HVFHV** &nbsp;|&nbsp; "
    f"Filter: **{bulan}** &nbsp;|&nbsp; Kategori: **{kategori}**"
)
st.markdown("---")

# ============================================================
# KPI
# ============================================================

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Trip",         f"{df_zone['trip_count'].sum():,.0f}")
k2.metric("Rata-rata Tarif",    f"${df_zone['avg_fare'].mean():.2f}")
k3.metric("Rata-rata Tips",     f"${df_zone['avg_tip'].mean():.2f}")
k4.metric("Rata-rata Jarak",    f"{df_zone['avg_distance'].mean():.2f} mil")
k5.metric("Zona Aktif",         f"{df_zone['PULocationID'].nunique()} zona")

st.markdown("---")

# ============================================================
# SECTION 1 — PETA
# ============================================================

st.header("🗺️ Peta Intensitas Pickup — Manhattan Yellow Zone")

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
    st.subheader("🔵 HVFHV — Uber & Lyft (Modern)")
    m2 = build_map(df_fhvhv, geojson, "HVFHV", "Blues")
    st_folium(m2, width=520, height=430, key="map_fhvhv")

st.markdown("---")

# ============================================================
# SECTION 2 — MARKET SHARE
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
# SECTION 3 — POLA WAKTU
# ============================================================

st.header("⏰ Pola Waktu — Volume Trip")

tab_line, tab_heat = st.tabs(["📈 Per Jam", "🔥 Heatmap Jam × Hari"])

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
        xaxis=dict(tickmode="linear", dtick=1, title="Jam (0–23)"),
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
        title="Heatmap Volume Trip — Jam × Hari",
        labels=dict(x="Jam", y="Hari", color="Jumlah Trip"),
        aspect="auto"
    )
    fig_heat.update_layout(height=350)
    st.plotly_chart(fig_heat, use_container_width=True)

st.markdown("---")

# ============================================================
# SECTION 4 — TREN HARIAN
# ============================================================

st.header("📅 Tren Harian — Januari s/d Maret 2026")

df_trend = load_trend_all(kategori)

# Garis vertikal batas bulan
bulan_batas = ["2026-02-01", "2026-03-01"]

fig_trend = px.line(
    df_trend,
    x="date", y="trip_count",
    color="category",
    color_discrete_map=CATEGORY_COLOR,
    title="Volume Trip Harian — Januari s/d Maret 2026",
    labels={"date": "Tanggal", "trip_count": "Jumlah Trip", "category": "Kategori"}
)

# Tambah garis pemisah bulan
for batas in bulan_batas:
    fig_trend.add_vline(
        x=batas,
        line_dash="dash",
        line_color="gray",
        opacity=0.5
    )

# Tambah label bulan
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
# SECTION 5 — ANALISIS TARIF
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
# SECTION 6 — TOP ZONA
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
        title="Top 10 Zona Pickup — Yellow Taxi",
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
        title="Top 10 Zona Pickup — HVFHV (Uber & Lyft)",
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
# SECTION 7 — TARIF PER JAM
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
    title="Rata-rata Tarif per Jam — Konvensional vs Modern",
    labels={"hour": "Jam", "avg_fare": "Rata-rata Tarif ($)", "category": "Kategori"}
)
fig_fare_hour.update_layout(
    xaxis=dict(tickmode="linear", dtick=1),
    height=380
)
st.plotly_chart(fig_fare_hour, use_container_width=True)