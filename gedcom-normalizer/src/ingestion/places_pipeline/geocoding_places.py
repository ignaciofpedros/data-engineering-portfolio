from pathlib import Path
import pandas as pd
from geopy.geocoders import Nominatim
from time import sleep
import json

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[3]
STAGING_DIR = BASE_DIR / "data/staging/places_aux"

INPUT_FILE = STAGING_DIR / "places_with_canonical.csv"  # <- ya con canonical
OUTPUT_FILE = STAGING_DIR / "places_geocoded.csv"
CACHE_FILE = STAGING_DIR / "geocode_cache.json"

# =========================
# LOAD DATA
# =========================

df = pd.read_csv(INPUT_FILE)

# IMPORTANTE: geocoding sobre canonical_place
places = (
    df["canonical_place"]
    .dropna()
    .astype(str)
    .str.strip()
    .unique()
    .tolist()
)

print(f"🔎 Unique canonical places: {len(places)}")

# =========================
# CACHE
# =========================

try:
    with open(CACHE_FILE, "r") as f:
        cache = json.load(f)
except FileNotFoundError:
    cache = {}

# =========================
# GEOCODER
# =========================

geolocator = Nominatim(user_agent="gedcom_geocoder_v2")

def geocode(place: str):
    if place in cache:
        return cache[place]

    try:
        location = geolocator.geocode(place, timeout=10)
        sleep(1)  # respetar rate limit (1 req/s)

        if location:
            result = {
                "canonical_place": place,
                "found": True,
                "lat": location.latitude,
                "lon": location.longitude,
                "display_name": location.address
            }
        else:
            result = {
                "canonical_place": place,
                "found": False,
                "lat": None,
                "lon": None,
                "display_name": None
            }

    except Exception as e:
        result = {
            "canonical_place": place,
            "found": False,
            "lat": None,
            "lon": None,
            "display_name": None,
            "error": str(e)
        }

    cache[place] = result

    # guardar cache incremental (evita perder progreso)
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    return result

# =========================
# RUN
# =========================

results = []

for i, place in enumerate(places, 1):
    print(f"[{i}/{len(places)}] {place}")
    results.append(geocode(place))

# =========================
# EXPORT
# =========================

geo_df = pd.DataFrame(results)

# confidence simple
geo_df["confidence"] = geo_df["found"].apply(lambda x: 1.0 if x else 0.0)

geo_df.to_csv(OUTPUT_FILE, index=False)

print("\n✅ Geocoding completado")
print(f"📁 Output: {OUTPUT_FILE}")
print(f"💾 Cache: {CACHE_FILE}")
print(f"✔ Found: {geo_df['found'].sum()}")
print(f"❌ Not found: {len(geo_df) - geo_df['found'].sum()}")