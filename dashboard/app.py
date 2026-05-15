import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import json

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="NYC TLC Dashboard",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================================================
# CUSTOM CSS
# =========================================================
st.markdown("""
<style>

@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

/* BACKGROUND */
.stApp {
    background-color: #f5f7fb;
}

/* SIDEBAR */
[data-testid="stSidebar"] {
    background-color: white;
    border-right: 1px solid #e5e7eb;
}

/* HEADER */
.dashboard-header {
    background: white;
    padding: 32px;
    border-radius: 20px;
    border: 1px solid #e5e7eb;
    margin-bottom: 24px;
    box-shadow: 0 2px 10px rgba(0,0,0,0.04);
}

.dashboard-title {
    font-size: 2.5rem;
    font-weight: 700;
    color: #111827;
}

.dashboard-subtitle {
    color: #6b7280;
    margin-top: 8px;
}

/* METRIC */
[data-testid="metric-container"] {
    background: white;
    border: 1px solid #e5e7eb;
    padding: 16px;
    border-radius: 16px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.03);
}

/* REMOVE STREAMLIT DEFAULT */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}

</style>
""", unsafe_allow_html=True)

# =========================================================
# LOAD DATA
# =========================================================
@st.cache_data
def load_data():

    con = duckdb.connect()

    PARQUET_PATH = "data/trips_clean.parquet"

    # =====================================================
    # DUMMY DATA
    # =====================================================
    if not os.path.exists(PARQUET_PATH):

        df = pd.DataFrame({

            "taxi_type":
                ["Yellow","Yellow","Green","Green","FHV","HVFHV","HVFHV"] * 200,

            "pickup_datetime":
                pd.date_range("2026-01-01", periods=1400, freq="h"),

            "dropoff_datetime":
                pd.date_range("2026-01-01 00:20", periods=1400, freq="h"),

            "PULocationID":
                ([1,2,3,4,5,6,7] * 200),

            "DOLocationID":
                ([7,6,5,4,3,2,1] * 200),

            "trip_distance":
                [2.5,5.1,3.3,1.2,4.0,6.7,2.1] * 200,

            "fare_amount":
                [10,18,13,7,15,22,9] * 200,

            "tip_amount":
                [1.5,3.0,0.0,0.5,2.0,4.0,1.0] * 200,
        })

        # FEATURE ENGINEERING
        df["bulan"] = df["pickup_datetime"].dt.month
        df["jam"] = df["pickup_datetime"].dt.hour
        df["hari_minggu"] = df["pickup_datetime"].dt.dayofweek

        df["durasi_menit"] = (
            (df["dropoff_datetime"] - df["pickup_datetime"])
            .dt.total_seconds() / 60
        )

        st.warning("⚠️ File parquet tidak ditemukan — menggunakan dummy data.")

        return df

    # =====================================================
    # REAL DATA
    # =====================================================
    query = f"""
    SELECT
        taxi_type,
        pickup_datetime,
        dropoff_datetime,
        PULocationID,
        DOLocationID,
        trip_distance,
        fare_amount,
        tip_amount,

        EXTRACT(month FROM pickup_datetime) AS bulan,
        EXTRACT(hour FROM pickup_datetime) AS jam,
        EXTRACT(dow FROM pickup_datetime) AS hari_minggu,

        DATE_DIFF(
            'minute',
            pickup_datetime,
            dropoff_datetime
        ) AS durasi_menit

    FROM read_parquet('{PARQUET_PATH}')
    """

    df = con.execute(query).df()

    return df


df = load_data()

# =========================================================
# SIDEBAR
# =========================================================
with st.sidebar:

    st.title("🚕 NYC TLC")

    st.markdown("### Filter Dashboard")

    moda_options = ["Yellow", "Green", "FHV", "HVFHV"]

    moda_selected = st.multiselect(
        "Pilih Moda",
        options=moda_options,
        default=moda_options
    )

    bulan_map = {
        1: "Januari",
        2: "Februari",
        3: "Maret",
        4: "April",
        5: "Mei",
        6: "Juni",
        7: "Juli",
        8: "Agustus",
        9: "September",
        10: "Oktober",
        11: "November",
        12: "Desember"
    }

    bulan_selected = st.multiselect(
        "Pilih Bulan",
        options=sorted(df["bulan"].unique()),
        default=sorted(df["bulan"].unique()),
        format_func=lambda x: bulan_map.get(x, x)
    )

    jam_range = st.slider(
        "Rentang Jam",
        0,
        23,
        (0, 23)
    )

    tipe_hari = st.radio(
        "Tipe Hari",
        ["Semua", "Hari Kerja", "Akhir Pekan"]
    )

# =========================================================
# FILTER DATA
# =========================================================
df_filtered = df[
    (df["taxi_type"].isin(moda_selected)) &
    (df["bulan"].isin(bulan_selected)) &
    (df["jam"] >= jam_range[0]) &
    (df["jam"] <= jam_range[1])
].copy()

if tipe_hari == "Hari Kerja":

    df_filtered = df_filtered[
        df_filtered["hari_minggu"].isin([0,1,2,3,4])
    ]

elif tipe_hari == "Akhir Pekan":

    df_filtered = df_filtered[
        df_filtered["hari_minggu"].isin([5,6])
    ]

# =========================================================
# HEADER
# =========================================================
st.markdown("""
<div class="dashboard-header">

<div class="dashboard-title">
🚕 NYC TLC Mobility Dashboard
</div>

<div class="dashboard-subtitle">
Analisis transportasi online dan konvensional di New York City
</div>

</div>
""", unsafe_allow_html=True)

# =========================================================
# KPI
# =========================================================
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Total Trips",
        f"{len(df_filtered):,}"
    )

with col2:
    st.metric(
        "Avg Distance",
        f"{df_filtered['trip_distance'].mean():.2f} mi"
    )

with col3:
    st.metric(
        "Avg Fare",
        f"${df_filtered['fare_amount'].mean():.2f}"
    )

with col4:
    st.metric(
        "Avg Duration",
        f"{df_filtered['durasi_menit'].mean():.1f} min"
    )

# =========================================================
# COLORS
# =========================================================
color_map = {
    "Yellow": "#facc15",
    "Green": "#22c55e",
    "FHV": "#3b82f6",
    "HVFHV": "#ef4444"
}

# =========================================================
# ROW 1
# =========================================================
col_a, col_b = st.columns([1, 2])

# ---------------------------------------------------------
# BAR CHART
# ---------------------------------------------------------
with col_a:

    with st.container(border=True):

        st.subheader("Volume Trip per Moda")

        volume = (
            df_filtered
            .groupby("taxi_type")
            .size()
            .reset_index(name="jumlah_trip")
        )

        fig_bar = px.bar(
            volume,
            x="taxi_type",
            y="jumlah_trip",
            color="taxi_type",
            color_discrete_map=color_map,
            template="simple_white"
        )

        fig_bar.update_layout(
            showlegend=False,
            height=420
        )

        st.plotly_chart(
            fig_bar,
            use_container_width=True
        )

# ---------------------------------------------------------
# LINE CHART
# ---------------------------------------------------------
with col_b:

    with st.container(border=True):

        st.subheader("Tren Bulanan")

        trend = (
            df_filtered
            .groupby(["bulan", "taxi_type"])
            .size()
            .reset_index(name="jumlah_trip")
        )

        trend["bulan_label"] = trend["bulan"].map({
            1: "Jan",
            2: "Feb",
            3: "Mar",
            4: "Apr",
            5: "Mei",
            6: "Jun",
            7: "Jul",
            8: "Agu",
            9: "Sep",
            10: "Okt",
            11: "Nov",
            12: "Des"
        })

        fig_line = px.line(
            trend,
            x="bulan_label",
            y="jumlah_trip",
            color="taxi_type",
            markers=True,
            color_discrete_map=color_map,
            template="simple_white"
        )

        fig_line.update_layout(
            height=420
        )

        st.plotly_chart(
            fig_line,
            use_container_width=True
        )

# =========================================================
# ROW 2
# =========================================================
col_c, col_d = st.columns(2)

# ---------------------------------------------------------
# JAM SIBUK
# ---------------------------------------------------------
with col_c:

    with st.container(border=True):

        st.subheader("Distribusi Jam Sibuk")

        jam_dist = (
            df_filtered
            .groupby(["jam", "taxi_type"])
            .size()
            .reset_index(name="jumlah_trip")
        )

        fig_hour = px.area(
            jam_dist,
            x="jam",
            y="jumlah_trip",
            color="taxi_type",
            color_discrete_map=color_map,
            template="simple_white"
        )

        fig_hour.update_layout(
            height=420
        )

        st.plotly_chart(
            fig_hour,
            use_container_width=True
        )

# ---------------------------------------------------------
# FARE & TIP
# ---------------------------------------------------------
with col_d:

    with st.container(border=True):

        st.subheader("Fare dan Tip")

        ekonomi = (
            df_filtered
            .groupby("taxi_type")
            .agg(
                avg_fare=("fare_amount", "mean"),
                avg_tip=("tip_amount", "mean")
            )
            .reset_index()
        )

        fig_eco = go.Figure()

        fig_eco.add_trace(go.Bar(
            name="Fare",
            x=ekonomi["taxi_type"],
            y=ekonomi["avg_fare"]
        ))

        fig_eco.add_trace(go.Bar(
            name="Tip",
            x=ekonomi["taxi_type"],
            y=ekonomi["avg_tip"]
        ))

        fig_eco.update_layout(
            template="simple_white",
            barmode="group",
            height=420
        )

        st.plotly_chart(
            fig_eco,
            use_container_width=True
        )

# =========================================================
# MAP
# =========================================================
with st.container(border=True):

    st.subheader("🗺️ Pickup Density Map")

    GEOJSON_PATH = "data/nyc_taxi_zones.geojson"

    if os.path.exists(GEOJSON_PATH):

        with open(GEOJSON_PATH) as f:
            geojson = json.load(f)

        zona_count = (
            df_filtered
            .groupby("PULocationID")
            .size()
            .reset_index(name="jumlah_trip")
        )

        zona_count["PULocationID"] = zona_count["PULocationID"].astype(str)

        fig_map = px.choropleth_mapbox(
            zona_count,
            geojson=geojson,
            locations="PULocationID",
            featureidkey="properties.LocationID",
            color="jumlah_trip",
            color_continuous_scale="Blues",
            mapbox_style="carto-positron",
            zoom=9,
            center={
                "lat": 40.7128,
                "lon": -74.0060
            },
            opacity=0.7,
            hover_data=["jumlah_trip"]
        )

        fig_map.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            height=700
        )

        st.plotly_chart(
            fig_map,
            use_container_width=True
        )

    else:

        st.info(
            "Tambahkan file nyc_taxi_zones.geojson ke folder data/"
        )

# =========================================================
# DATA TABLE
# =========================================================
with st.container(border=True):

    st.subheader("Preview Dataset")

    st.dataframe(
        df_filtered.head(50),
        use_container_width=True
    )

# =========================================================
# FOOTER
# =========================================================
st.caption(
    "📊 NYC TLC Analytics Dashboard | Streamlit + DuckDB + Plotly"
)