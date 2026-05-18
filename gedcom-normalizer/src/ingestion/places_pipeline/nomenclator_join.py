from pathlib import Path
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
nomenclator_input = AUX_DIR / "nomenclator.csv"

output_file = AUX_DIR / "places_matched.csv"

# =========================
# CONFIG
# =========================

MATCH_THRESHOLD = 70

# =========================
# NORMALIZACIÓN
# =========================


def normalize_text(text: str) -> str:
    if pd.isna(text) or text is None:
        return None

    text = str(text).strip().lower()

    # quitar acentos
    text = unidecode.unidecode(text)

    # normalizar separadores
    text = text.replace(";", ",")
    text = text.replace("/", ",")

    # quitar caracteres raros
    text = re.sub(r"[^\w\s,.-]", " ", text)

    # espacios repetidos
    text = re.sub(r"\s+", " ", text)

    # normalizar comas
    text = re.sub(r"\s*,\s*", ", ", text)

    # quitar comas repetidas
    text = re.sub(r",+", ",", text)

    return text.strip(" ,")


# =========================
# CARGAR DATOS
# =========================

places_df = pd.read_csv(places_norm_input)
nomen_df = pd.read_csv(nomenclator_input)

# =========================
# NORMALIZAR NOMENCLÁTOR
# =========================

nomen_columns = [
    "provincia",
    "concello",
    "parroquia",
    "advocacion parroquia",
    "lugar"
]

for col in nomen_columns:
    nomen_df[col] = nomen_df[col].apply(normalize_text)

# =========================
# CREAR CAMPOS JERÁRQUICOS
# =========================


def build_full_path(row):
    parts = [
        row.get("lugar"),
        row.get("parroquia"),
        row.get("concello"),
        row.get("provincia")
    ]

    parts = [p for p in parts if pd.notna(p) and p]

    return ", ".join(parts)


nomen_df["full_path"] = nomen_df.apply(build_full_path, axis=1)

# =========================
# MATCHING
# =========================


def score_match(place_clean, place_parts, nomen_row):
    """
    Score compuesto jerárquico.
    """

    score = 0

    full_path = nomen_row["full_path"]
    lugar = nomen_row["lugar"]
    parroquia = nomen_row["parroquia"]
    concello = nomen_row["concello"]
    provincia = nomen_row["provincia"]

    # =========================
    # 1. similitud global
    # =========================

    global_score = fuzz.WRatio(place_clean, full_path)

    score += global_score * 0.40

    # =========================
    # 2. matching por niveles
    # =========================

    if len(place_parts) >= 1 and lugar:
        s = fuzz.WRatio(place_parts[0], lugar)
        score += s * 0.30

    if len(place_parts) >= 2 and parroquia:
        s = fuzz.WRatio(place_parts[1], parroquia)
        score += s * 0.15

    if len(place_parts) >= 2 and concello:
        s = fuzz.WRatio(place_parts[-1], concello)
        score += s * 0.10

    if len(place_parts) >= 3 and provincia:
        s = fuzz.WRatio(place_parts[-1], provincia)
        score += s * 0.05

    return round(score / 100, 4)


results = []

for _, place_row in places_df.iterrows():

    place_clean = place_row["place_clean"]

    if pd.isna(place_clean):
        continue

    place_parts = [
        p.strip()
        for p in place_clean.split(",")
        if p.strip()
    ]

    best_score = 0
    best_match = None

    # =========================
    # FILTRADO JERÁRQUICO
    # =========================

    candidates = nomen_df

    # intentar reducir candidatos por concello
    if len(place_parts) >= 2:
        possible_concello = place_parts[-1]

        filtered = nomen_df[
            nomen_df["concello"]
            .fillna("")
            .str.contains(possible_concello, regex=False)
        ]

        if len(filtered) > 0:
            candidates = filtered

    # =========================
    # MATCHING
    # =========================

    for _, nomen_row in candidates.iterrows():

        score = score_match(
            place_clean,
            place_parts,
            nomen_row
        )

        if score > best_score:
            best_score = score
            best_match = nomen_row

    # =========================
    # RESULTADO
    # =========================

    result = place_row.to_dict()

    result["match_score"] = round(best_score * 100, 2)

    if best_match is not None and best_score * 100 >= MATCH_THRESHOLD:

        result["canonical_place"] = best_match["full_path"]
        result["canonical_lugar"] = best_match["lugar"]
        result["canonical_parroquia"] = best_match["parroquia"]
        result["canonical_concello"] = best_match["concello"]
        result["canonical_provincia"] = best_match["provincia"]
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
# EXPORTAR
# =========================

results_df = pd.DataFrame(results)

results_df = results_df.sort_values(
    by="match_score",
    ascending=False
)

results_df.to_csv(output_file, index=False)

# =========================
# STATS
# =========================

matched_count = results_df["matched"].sum()

total = len(results_df)

print("✅ Matching completado")
print(f"Output: {output_file}")

print(f"\nTotal lugares: {total}")
print(f"Matches: {matched_count}")
print(f"Sin match: {total - matched_count}")
print(f"Ratio match: {(matched_count / total) * 100:.2f}%")