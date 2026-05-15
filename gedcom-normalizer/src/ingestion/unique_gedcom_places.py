from pathlib import Path
import pandas as pd
import unidecode
import re

BASE_DIR = Path(__file__).resolve().parents[2]
STAGING_DIR = BASE_DIR / "data/staging"

places_input = STAGING_DIR / "places.csv"
places_norm_output = STAGING_DIR / "places_normalized.csv"

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

    # quitar dobles espacios
    text = re.sub(r"\s+", " ", text)

    # quitar espacios alrededor de comas
    text = re.sub(r"\s*,\s*", ", ", text)

    # quitar puntuación repetida
    text = re.sub(r",+", ",", text)

    return text.strip(" ,")

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

norm_df["place_clean"] = norm_df["place_raw"].apply(clean_place)

# placeholder para futuras fases
norm_df["canonical_place"] = None
norm_df["lat"] = None
norm_df["lon"] = None
norm_df["confidence"] = None
norm_df["reviewed"] = False

# =========================
# EXPORTAR
# =========================

norm_df.to_csv(places_norm_output, index=False)

print("✅ Tabla de normalización creada")
print(f"Output: {places_norm_output}")

print(f"\nTotal lugares originales: {len(df)}")
print(f"Lugares únicos: {len(norm_df)}")