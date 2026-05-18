from pathlib import Path
import pandas as pd

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[3]
STAGING_DIR = BASE_DIR / "data/staging/places_aux"

INPUT_FILE = STAGING_DIR / "places_clusters.csv"
OUTPUT_FILE = STAGING_DIR / "places_with_canonical.csv"

# =========================
# LOAD
# =========================

df = pd.read_csv(INPUT_FILE)

# seguridad
df["place_clean"] = df["place_clean"].astype(str)
df["canonical_candidate"] = df["canonical_candidate"].astype(str)

# =========================
# FUNCTION: elegir canonical por cluster
# =========================

def choose_canonical(group: pd.DataFrame) -> str:
    """
    Estrategia:
    1. el más frecuente
    2. si empate, el más largo
    3. fallback: canonical_candidate
    """

    # frecuencia de place_clean dentro del cluster
    freq = group["place_clean"].value_counts()

    top_freq = freq.max()
    candidates = freq[freq == top_freq].index.tolist()

    if len(candidates) == 1:
        return candidates[0]

    # si hay empate → elegir el más descriptivo (más largo)
    candidates_sorted = sorted(candidates, key=len, reverse=True)

    return candidates_sorted[0]

# =========================
# APPLY POR CLUSTER
# =========================

canonical_map = (
    df.groupby("cluster_id")
    .apply(choose_canonical)
    .to_dict()
)

df["canonical_place"] = df["cluster_id"].map(canonical_map)

# =========================
# (OPCIONAL) mejora semántica ligera
# =========================
# si canonical_candidate es más rico que place_clean, usarlo

def improve(row):
    pc = row["place_clean"]
    cc = row["canonical_candidate"]

    # si el candidato es claramente más informativo, usarlo
    if len(cc) > len(pc) * 1.3:
        return cc

    return row["canonical_place"]

df["canonical_place"] = df.apply(improve, axis=1)

# =========================
# EXPORT
# =========================

df.to_csv(OUTPUT_FILE, index=False)

# =========================
# STATS
# =========================

print("\n✅ Canonical places generados")
print(f"Output: {OUTPUT_FILE}")

print(f"\nClusters procesados: {df['cluster_id'].nunique()}")
print(f"Rows: {len(df)}")

print("\nEjemplos:")
print(df[["cluster_id", "place_clean", "canonical_candidate", "canonical_place"]].head(10))