import duckdb
import pandas as pd

con = duckdb.connect("nyc_taxi_manhattan.duckdb")

print("=== SCREENSHOT 1: DAFTAR TABEL ===")
print(con.execute("SHOW TABLES").df().to_string(index=False))

print()
print("=== SCREENSHOT 2: STRUKTUR fact_trips ===")
print(con.execute("DESCRIBE fact_trips").df().to_string(index=False))

print()
print("=== JUMLAH BARIS PER TABEL ===")

result = []

tables = {
    "dim_location": "SELECT COUNT(*) FROM dim_location",
    "dim_operator": "SELECT COUNT(*) FROM dim_operator",
    "fact_trips": "SELECT COUNT(*) FROM fact_trips",
    "agg_hourly_demand": "SELECT COUNT(*) FROM agg_hourly_demand",
    "agg_zone_demand": "SELECT COUNT(*) FROM agg_zone_demand",
}

for tabel, query in tables.items():
    jumlah = con.execute(query).fetchone()[0]
    result.append({
        "tabel": tabel,
        "jumlah_baris": f"{jumlah:,}"
    })

print(pd.DataFrame(result).to_string(index=False))

print()
print("=== ISI dim_operator ===")
print(con.execute("SELECT * FROM dim_operator").df().to_string(index=False))

print()
print("=== SAMPLE fact_trips (5 baris) ===")
print(con.execute("""
    SELECT
        trip_id,
        operator_code,
        PULocationID,
        DOLocationID,
        pickup_datetime,
        trip_duration_min,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount
    FROM fact_trips
    LIMIT 5
""").df().to_string(index=False))

print()
print("=== DISTRIBUSI TRIP PER OPERATOR ===")
print(con.execute("""
    SELECT
        o.category,
        o.operator_name,
        COUNT(*) AS total_trip,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS persen
    FROM fact_trips f
    JOIN dim_operator o ON f.operator_code = o.operator_code
    GROUP BY o.category, o.operator_name
    ORDER BY total_trip DESC
""").df().to_string(index=False))

con.close()