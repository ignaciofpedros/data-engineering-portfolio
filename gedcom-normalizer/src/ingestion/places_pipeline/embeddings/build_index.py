from pathlib import Path
import pickle

import faiss
import numpy as np
import pandas as pd

from sentence_transformers import SentenceTransformer

from ingestion.places_pipeline.normalize import (
    normalize_text
)

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[4]

STAGING_DIR = BASE_DIR / "data/staging"
AUX_DIR = STAGING_DIR / "places_aux"

NOMENCLATOR_PATH = (
    AUX_DIR / "nomenclator_transformado.csv"
)

INDEX_PATH = AUX_DIR / "faiss_index.bin"

META_PATH = AUX_DIR / "faiss_meta.pkl"

# =========================
# CONFIG
# =========================

EMBEDDING_MODEL = "BAAI/bge-m3"

# =========================
# BUILD TEXT
# =========================

def build_semantic_text(row):

    parts = [
        row.get("lugar"),
        row.get("parroquia"),
        row.get("concello"),
        row.get("provincia")
    ]

    parts = [
        normalize_text(p)
        for p in parts
        if pd.notna(p) and p
    ]

    return ", ".join(parts)

# =========================
# MAIN
# =========================

def build_index():

    print("\n📥 Loading nomenclator...")

    df = pd.read_csv(NOMENCLATOR_PATH)

    # =========================
    # NORMALIZATION
    # =========================

    cols = [
        "provincia",
        "concello",
        "parroquia",
        "lugar"
    ]

    for c in cols:

        if c in df.columns:

            df[c] = df[c].apply(
                normalize_text
            )

    # =========================
    # SEMANTIC TEXT
    # =========================

    print("🧠 Building semantic text...")

    df["semantic_text"] = df.apply(
        build_semantic_text,
        axis=1
    )

    # Eliminamos filas vacías
    df = df[
        df["semantic_text"]
        .notna()
    ]

    df = df[
        df["semantic_text"]
        .str.len() > 0
    ]

    texts = df["semantic_text"].tolist()

    print(f"✅ Total semantic rows: {len(texts)}")

    # =========================
    # LOAD MODEL
    # =========================

    print(f"\n🤖 Loading model: {EMBEDDING_MODEL}")

    model = SentenceTransformer(
        EMBEDDING_MODEL
    )

    # =========================
    # GENERATE EMBEDDINGS
    # =========================

    print("\n⚡ Generating embeddings...")

    embeddings = model.encode(
        texts,
        show_progress_bar=True,
        normalize_embeddings=True
    )

    embeddings = np.array(
        embeddings
    ).astype("float32")

    # =========================
    # BUILD FAISS INDEX
    # =========================

    print("\n📦 Building FAISS index...")

    dimension = embeddings.shape[1]

    index = faiss.IndexFlatIP(
        dimension
    )

    index.add(embeddings)

    # =========================
    # SAVE INDEX
    # =========================

    print("\n💾 Saving index...")

    faiss.write_index(
        index,
        str(INDEX_PATH)
    )

    # =========================
    # SAVE METADATA
    # =========================

    metadata = df.to_dict(
        orient="records"
    )

    with open(META_PATH, "wb") as f:
        pickle.dump(metadata, f)

    # =========================
    # DONE
    # =========================

    print("\n✅ FAISS index created")
    print(f"Index path: {INDEX_PATH}")
    print(f"Metadata path: {META_PATH}")
    print(f"Vectors indexed: {len(metadata)}")


# =========================
# ENTRYPOINT
# =========================

if __name__ == "__main__":

    build_index()