from pathlib import Path
import pandas as pd
import unidecode
import re

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
# CONFIG
# =========================

ADVOCATION_PREFIXES = [
    "san",
    "santa",
    "santo",
    "sant",
    "santa maria",
    "san xoan",
    "san pedro",
    "san martino"
]

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

    # normalizar comas
    text = re.sub(r"\s*,\s*", ", ", text)

    # quitar comas repetidas
    text = re.sub(r",+", ",", text)

    return text.strip(" ,")


def split_place(text: str):
    if not text:
        return []

    return [p.strip() for p in text.split(",") if p.strip()]


def detect_advocation(parts):
    """
    Detecta posibles advocaciones parroquiales.
    """

    for part in parts:
        for prefix in ADVOCATION_PREFIXES:
            if part.startswith(prefix):
                return True, prefix

    return False, None

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

# limpiar
norm_df["place_clean"] = norm_df["place_raw"].apply(clean_place)

# trocear
norm_df["place_parts"] = norm_df["place_clean"].apply(split_place)

# número de partes
norm_df["place_num_parts"] = norm_df["place_parts"].apply(len)

# columnas individuales
MAX_PARTS = 6

for i in range(MAX_PARTS):
    norm_df[f"place_part_{i+1}"] = norm_df["place_parts"].apply(
        lambda x: x[i] if len(x) > i else None
    )

# detectar advocación
adv_results = norm_df["place_parts"].apply(detect_advocation)

norm_df["has_advocation"] = adv_results.apply(lambda x: x[0])
norm_df["possible_advocation"] = adv_results.apply(lambda x: x[1])

# placeholders matching
norm_df["canonical_place"] = None
norm_df["canonical_id"] = None
norm_df["lat"] = None
norm_df["lon"] = None
norm_df["confidence"] = None
norm_df["reviewed"] = False

# opcional: eliminar lista auxiliar
norm_df = norm_df.drop(columns=["place_parts"])

# =========================
# EXPORTAR
# =========================

norm_df.to_csv(places_norm_output, index=False)

print("✅ Tabla de normalización creada")
print(f"Output: {places_norm_output}")

print(f"\nTotal lugares originales: {len(df)}")
print(f"Lugares únicos: {len(norm_df)}")