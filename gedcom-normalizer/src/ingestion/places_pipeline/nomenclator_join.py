from pathlib import Path
from collections import defaultdict

import pandas as pd
import unidecode
import re

from rapidfuzz import fuzz

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[3]

STAGING_DIR = BASE_DIR / "data/staging"
AUX_DIR = STAGING_DIR / "places_aux"

places_norm_input = AUX_DIR / "places_normalized.csv"
nomenclator_input = AUX_DIR / "nomenclator_transformado.csv"

output_file = AUX_DIR / "places_matched.csv"

# =========================
# CONFIG
# =========================

MATCH_THRESHOLD = 60.0

WEIGHT_GLOBAL = 0.55
WEIGHT_LUGAR = 0.05
WEIGHT_PARROQUIA = 0.20
WEIGHT_CONCELLO = 0.20

# =========================
# NORMALIZACIÓN
# =========================

def normalize_text(text: str) -> str:

    if pd.isna(text) or text is None:
        return None

    text = str(text).strip().lower()

    if not text:
        return None

    text = unidecode.unidecode(text)

    text = text.replace(";", ",")
    text = text.replace("/", ",")

    text = re.sub(r"[^\w\s,.-]", " ", text)
    text = re.sub(r"\s+", " ", text)

    text = re.sub(r"\s*,\s*", ", ", text)
    text = re.sub(r",+", ",", text)

    text = text.strip(" ,")

    return text if text else None


# =========================
# LOAD
# =========================

places_df = pd.read_csv(places_norm_input)
nomen_df = pd.read_csv(nomenclator_input)

# =========================
# NORMALIZAR NOMENCLATOR
# =========================

cols = [
    "provincia",
    "concello",
    "parroquia",
    "lugar"
]

for c in cols:
    nomen_df[c] = nomen_df[c].apply(normalize_text)

# =========================
# FULL PATH
# =========================

def build_full_path(row):

    parts = [
        row.get("lugar"),
        row.get("parroquia"),
        row.get("concello"),
        row.get("provincia")
    ]

    parts = [
        p for p in parts
        if pd.notna(p) and p
    ]

    return ", ".join(parts) if parts else None


nomen_df["full_path"] = nomen_df.apply(
    build_full_path,
    axis=1
)

# =========================
# PRECOMPUTE RECORDS
# =========================

nomen_records = nomen_df.to_dict("records")

# Índice rápido por concello
nomen_by_concello = defaultdict(list)

for rec in nomen_records:

    concello = rec.get("concello")

    if concello:
        nomen_by_concello[concello].append(rec)

# =========================
# SCORE
# =========================

def score_match(place_clean, place_parts, row):

    full_path = row.get("full_path")

    if not full_path:
        return 0

    score = 0

    # =========================
    # GLOBAL
    # =========================

    score += (
        fuzz.ratio(place_clean, full_path)
        * WEIGHT_GLOBAL
    )

    # =========================
    # LUGAR
    # =========================

    lugar = row.get("lugar")

    if lugar and len(place_parts) >= 1:

        score += (
            fuzz.ratio(place_parts[0], lugar)
            * WEIGHT_LUGAR
        )

    # =========================
    # PARROQUIA
    # =========================

    parroquia = row.get("parroquia")

    if parroquia:

        score += (
            fuzz.ratio(place_clean, parroquia)
            * WEIGHT_PARROQUIA
        )

    # =========================
    # CONCELLO
    # =========================

    concello = row.get("concello")

    if concello and len(place_parts) >= 2:

        score += (
            fuzz.ratio(place_parts[-1], concello)
            * WEIGHT_CONCELLO
        )

    return score / 100


# =========================
# MATCHING
# =========================

results = []

for _, row in places_df.iterrows():

    place_clean = row.get("place_clean")

    # -------------------------
    # Validación inicial
    # -------------------------

    if pd.isna(place_clean) or not place_clean:
        continue

    place_clean = normalize_text(place_clean)

    if not place_clean:
        continue

    place_parts = [
        p.strip()
        for p in place_clean.split(",")
        if p.strip()
    ]

    # =========================
    # CANDIDATOS
    # =========================

    candidates = nomen_records

    # Filtro rápido por concello
    if len(place_parts) >= 2:

        possible_concello = place_parts[-1]

        filtered = nomen_by_concello.get(
            possible_concello
        )

        if filtered:
            candidates = filtered

    # =========================
    # BEST MATCH
    # =========================

    best_score = 0
    best_match = None

    for cand in candidates:

        s = score_match(
            place_clean,
            place_parts,
            cand
        )

        if s > best_score:
            best_score = s
            best_match = cand

    # =========================
    # SCORE FINAL
    # =========================

    final_score = round(
        best_score * 100,
        2
    )

    # =========================
    # RESULT
    # =========================

    result = row.to_dict()

    result["match_score"] = final_score

    if (
        best_match is not None
        and final_score >= MATCH_THRESHOLD
    ):

        result["canonical_place"] = (
            best_match["full_path"]
        )

        result["canonical_lugar"] = (
            best_match["lugar"]
        )

        result["canonical_parroquia"] = (
            best_match["parroquia"]
        )

        result["canonical_concello"] = (
            best_match["concello"]
        )

        result["canonical_provincia"] = (
            best_match["provincia"]
        )

        result["matched"] = True

    else:

        result["canonical_place"] = None
        result["canonical_lugar"] = None
        result["canonical_parroquia"] = None
        result["canonical_concello"] = None
        result["canonical_provincia"] = None

        result["matched"] = False

    results.append(result)

# =========================
# OUTPUT
# =========================

results_df = pd.DataFrame(results)

results_df.to_csv(
    output_file,
    index=False
)

# =========================
# STATS
# =========================

matched = results_df["matched"].sum()
total = len(results_df)

print("\n✅ Matching completado")
print(f"Total: {total}")
print(f"Matched: {matched}")
print(f"Ratio: {matched / total * 100:.2f}%")
print(f"No matched: {total - matched}")