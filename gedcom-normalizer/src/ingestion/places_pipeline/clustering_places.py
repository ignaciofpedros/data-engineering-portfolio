from pathlib import Path
import pandas as pd
import re
import unidecode
from rapidfuzz import fuzz, process

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[2]
STAGING_DIR = BASE_DIR / "data/staging"

INPUT_FILE = STAGING_DIR / "places_normalized.csv"
OUTPUT_FILE = STAGING_DIR / "places_clusters.csv"

# =========================
# CONFIG
# =========================

FUZZY_THRESHOLD = 90
MAX_MATCHES_PER_PLACE = 10

# =========================
# CLEANING
# =========================

def normalize_text(text: str) -> str:
    if pd.isna(text):
        return ""

    text = str(text).lower().strip()

    # quitar acentos
    text = unidecode.unidecode(text)

    # separadores
    text = text.replace(";", ",")
    text = text.replace("/", ",")

    # espacios
    text = re.sub(r"\s+", " ", text)

    # limpiar comas
    text = re.sub(r"\s*,\s*", ", ", text)
    text = re.sub(r",+", ",", text)

    return text.strip(" ,")

# =========================
# LOAD
# =========================

df = pd.read_csv(INPUT_FILE)

# asegurar limpieza
df["place_clean"] = df["place_clean"].fillna("").apply(normalize_text)

# únicos
places = (
    df["place_clean"]
    .dropna()
    .unique()
    .tolist()
)

print(f"Total unique places: {len(places)}")

# =========================
# BLOCKING
# =========================
# Reducimos comparaciones agrupando
# por primeras letras

blocks = {}

for place in places:
    key = place[:3]  # primeras 3 letras
    blocks.setdefault(key, []).append(place)

# =========================
# FUZZY CLUSTERING
# =========================

visited = set()
clusters = []
cluster_id = 1

for block_key, block_places in blocks.items():

    print(f"Processing block: {block_key} ({len(block_places)} places)")

    for place in block_places:

        if place in visited:
            continue

        visited.add(place)

        matches = process.extract(
            place,
            block_places,
            scorer=fuzz.token_sort_ratio,
            limit=MAX_MATCHES_PER_PLACE
        )

        cluster_members = []

        for match_place, score, _ in matches:

            if score >= FUZZY_THRESHOLD:
                cluster_members.append((match_place, score))
                visited.add(match_place)

        # ordenar por score descendente
        cluster_members = sorted(
            cluster_members,
            key=lambda x: x[1],
            reverse=True
        )

        # elegir canonical
        canonical = cluster_members[0][0]

        for member, score in cluster_members:

            clusters.append({
                "cluster_id": cluster_id,
                "canonical_candidate": canonical,
                "place_clean": member,
                "similarity_score": score,
                "needs_review": score < 100
            })

        cluster_id += 1

# =========================
# EXPORT
# =========================

clusters_df = pd.DataFrame(clusters)

clusters_df = clusters_df.sort_values(
    ["cluster_id", "similarity_score"],
    ascending=[True, False]
)

clusters_df.to_csv(OUTPUT_FILE, index=False)

# =========================
# STATS
# =========================

print("\n✅ Clustering completado")
print(f"Output: {OUTPUT_FILE}")

print(f"\nTotal clusters: {clusters_df['cluster_id'].nunique()}")

review_count = clusters_df["needs_review"].sum()

print(f"Rows needing review: {review_count}")