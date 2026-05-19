from pathlib import Path
import pandas as pd
import unidecode
import re

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[3]

STAGING_DIR = BASE_DIR / "data/staging"
AUX_DIR = STAGING_DIR / "places_aux"

places_input = STAGING_DIR / "places.csv"

places_norm_output = AUX_DIR / "places_normalized.csv"

# =========================
# CARGAR CSV
# =========================

df = pd.read_csv(places_input)

# =========================
# FUNCIONES LIMPIEZA
# =========================

def clean_place(text: str) -> str:

    if pd.isna(text):
        return None

    text = str(text).strip().lower()

    # quitar acentos
    text = unidecode.unidecode(text)

    # normalizar separadores
    text = text.replace(";", ",")
    text = text.replace("/", ",")

    # quitar caracteres raros
    text = re.sub(r"[^\w\s,.-]", " ", text)

    # quitar espacios repetidos
    text = re.sub(r"\s+", " ", text)

    # normalizar espacios alrededor comas
    text = re.sub(r"\s*,\s*", ", ", text)

    # quitar comas repetidas
    text = re.sub(r",+", ",", text)

    return text.strip(" ,")


# =========================
# SPLIT PLACE
# =========================

def split_place(text: str):

    if not text:
        return []

    return [
        p.strip()
        for p in text.split(",")
        if p.strip()
    ]


# =========================
# DETECTAR ADVOCACIÓN
# =========================

def detect_advocation(parts):
    """
    Detecta:
    - san xoan
    - santa maria
    - santo adrian

    y extrae:
    - probable parroquia
    """

    ADVOCATION_STARTERS = {
        "san",
        "santa",
        "santo",
        "sant"
    }

    for part in parts:

        words = part.split()

        if len(words) < 2:
            continue

        # comprobar inicio religioso
        if words[0] in ADVOCATION_STARTERS:

            # advocación completa:
            # san xoan
            # santa maria

            advocation = f"{words[0]} {words[1]}"

            probable_parroquia = None

            # detectar:
            # san xoan de ouces
            # santa maria de vigo

            if " de " in part:

                probable_parroquia = (
                    part.split(" de ", 1)[1]
                    .strip()
                )

            return {
                "has_advocation": True,
                "advocation": advocation,
                "probable_parroquia": probable_parroquia
            }

    return {
        "has_advocation": False,
        "advocation": None,
        "probable_parroquia": None
    }


# =========================
# EXTRAER ÚNICOS
# =========================

unique_places = (
    df["place_raw"]
    .dropna()
    .astype(str)
    .str.strip()
    .drop_duplicates()
)

# =========================
# TABLA NORMALIZACIÓN
# =========================

norm_df = pd.DataFrame({
    "place_raw": unique_places
})

# =========================
# LIMPIAR
# =========================

norm_df["place_clean"] = (
    norm_df["place_raw"]
    .apply(clean_place)
)

# =========================
# TROCEAR
# =========================

norm_df["place_parts"] = (
    norm_df["place_clean"]
    .apply(split_place)
)

# =========================
# NÚMERO PARTES
# =========================

norm_df["place_num_parts"] = (
    norm_df["place_parts"]
    .apply(len)
)

# =========================
# COLUMNAS PARTES
# =========================

MAX_PARTS = 6

for i in range(MAX_PARTS):

    norm_df[f"place_part_{i+1}"] = (
        norm_df["place_parts"]
        .apply(
            lambda x: x[i]
            if len(x) > i
            else None
        )
    )

# =========================
# ADVOCACIONES
# =========================

adv_results = (
    norm_df["place_parts"]
    .apply(detect_advocation)
)

norm_df["has_advocation"] = (
    adv_results.apply(
        lambda x: x["has_advocation"]
    )
)

norm_df["possible_advocation"] = (
    adv_results.apply(
        lambda x: x["advocation"]
    )
)

norm_df["probable_parroquia"] = (
    adv_results.apply(
        lambda x: x["probable_parroquia"]
    )
)

# =========================
# FEATURES AUXILIARES
# =========================

norm_df["token_count"] = (
    norm_df["place_clean"]
    .fillna("")
    .apply(
        lambda x: len(x.split())
    )
)

norm_df["char_length"] = (
    norm_df["place_clean"]
    .fillna("")
    .apply(len)
)

# =========================
# PLACEHOLDERS MATCHING
# =========================

norm_df["canonical_place"] = None
norm_df["canonical_id"] = None

norm_df["lat"] = None
norm_df["lon"] = None

norm_df["confidence"] = None

norm_df["reviewed"] = False

# =========================
# LIMPIEZA FINAL
# =========================

norm_df = norm_df.drop(
    columns=["place_parts"]
)

# =========================
# EXPORTAR
# =========================

norm_df.to_csv(
    places_norm_output,
    index=False
)

# =========================
# STATS
# =========================

print("✅ Tabla de normalización creada")

print(f"Output: {places_norm_output}")

print(f"\nTotal lugares originales: {len(df)}")

print(f"Lugares únicos: {len(norm_df)}")