from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = ROOT_DIR / "data" / "raw"
CLEAN_DIR = ROOT_DIR / "data" / "clean"
ZONE_CSV = ROOT_DIR / "taxi_zone_lookup.csv"
DB_PATH = str(ROOT_DIR / "nyc_taxi_manhattan.duckdb")

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

MONTHS = ["2026-01", "2026-02", "2026-03"]

MANHATTAN_YELLOW_ZONE_IDS = (
    4, 12, 13, 24, 43, 45, 48, 50, 68, 79,
    87, 88, 90, 100, 103, 104, 105, 107, 113, 114,
    125, 137, 140, 141, 142, 143, 144, 148, 151, 158,
    161, 162, 163, 164, 170, 186, 194, 209, 211, 224,
    229, 230, 231, 232, 233, 234, 236, 237, 238, 239,
    246, 249, 261, 262, 263
)