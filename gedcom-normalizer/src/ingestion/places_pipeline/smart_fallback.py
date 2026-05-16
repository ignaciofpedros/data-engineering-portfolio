from pathlib import Path
import pandas as pd
from rapidfuzz import process, fuzz

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[2]
STAGING_DIR = BASE_DIR / "data/staging"

INPUT_FILE = STAGING_DIR / "places_geocoded.csv"
OUTPUT_FILE = STAGING_DIR / "places_geocoded_filled.csv"


# df_geo: canonical_place + lat/lon (algunos vacíos)
df = pd.read_csv(INPUT_FILE)

known = df[df["found"] == True].dropna(subset=["lat", "lon"])
unknown = df[df["found"] == False]

known_places = known["canonical_place"].tolist()

def find_best_match(place):
    match = process.extractOne(
        place,
        known_places,
        scorer=fuzz.token_sort_ratio
    )

    if match:
        return match[0], match[1]
    return None, 0

resolved = []

for _, row in unknown.iterrows():
    place = row["canonical_place"]

    best_match, score = find_best_match(place)

    if score >= 80:
        match_row = known[known["canonical_place"] == best_match].iloc[0]

        resolved.append({
            "canonical_place": place,
            "lat": match_row["lat"],
            "lon": match_row["lon"],
            "inferred_from": best_match,
            "confidence": score
        })
    else:
        resolved.append({
            "canonical_place": place,
            "lat": None,
            "lon": None,
            "inferred_from": None,
            "confidence": score
        })

df_resolved = pd.DataFrame(resolved)

df_resolved.to_csv(OUTPUT_FILE, index=False)

print("✅ Fallback geográfico completado")