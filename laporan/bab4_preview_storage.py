import duckdb

con = duckdb.connect("nyc_taxi_manhattan.duckdb")

print("\n================ SAMPLE CLEAN PARQUET =================\n")

df_clean = con.execute("""
SELECT *
FROM read_parquet('data/clean/yellow_tripdata_2026-01.parquet')
LIMIT 10
""").df()

print(df_clean)

print("\n================ SAMPLE AGG HOURLY =================\n")

df_hourly = con.execute("""
SELECT *
FROM agg_hourly_demand
LIMIT 10
""").df()

print(df_hourly)

print("\n================ SAMPLE AGG ZONE =================\n")

df_zone = con.execute("""
SELECT *
FROM agg_zone_demand
LIMIT 10
""").df()

print(df_zone)