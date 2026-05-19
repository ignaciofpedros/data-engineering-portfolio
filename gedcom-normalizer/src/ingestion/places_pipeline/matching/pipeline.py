# src/ingestion/places_pipeline/matching/pipeline.py

from pathlib import Path
import json
import pickle

import faiss
import numpy as np
import pandas as pd
import requests

from sentence_transformers import SentenceTransformer

# =========================
# CONFIG
# =========================

EMBEDDING_MODEL = "BAAI/bge-m3"

OLLAMA_MODEL = "llama3.1:8b"

OLLAMA_URL = OLLAMA_URL = "http://host.docker.internal:11434/api/generate"

TOP_K = 15

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[4]

STAGING_DIR = BASE_DIR / "data/staging"
AUX_DIR = STAGING_DIR / "places_aux"

INDEX_PATH = AUX_DIR / "faiss_index.bin"
META_PATH = AUX_DIR / "faiss_meta.pkl"

# =========================
# LOAD MODEL
# =========================

print("\n🤖 Loading embedding model...")

embedder = SentenceTransformer(
    EMBEDDING_MODEL
)

# =========================
# LOAD FAISS
# =========================

print("\n📦 Loading FAISS index...")

index = faiss.read_index(
    str(INDEX_PATH)
)

# =========================
# LOAD METADATA
# =========================

print("\n📚 Loading metadata...")

with open(META_PATH, "rb") as f:

    metadata = pickle.load(f)

# =========================
# RETRIEVE CANDIDATES
# =========================

def retrieve_candidates(place_clean):

    emb = embedder.encode(
        [place_clean],
        normalize_embeddings=True
    )

    emb = np.array(
        emb
    ).astype("float32")

    distances, idxs = index.search(
        emb,
        TOP_K
    )

    candidates = []

    for i, idx in enumerate(idxs[0]):

        row = metadata[idx]

        candidate = {
            "score": float(distances[0][i]),
            "lugar": row.get("lugar"),
            "parroquia": row.get("parroquia"),
            "concello": row.get("concello"),
            "provincia": row.get("provincia"),
            "semantic_text": row.get("semantic_text")
        }

        candidates.append(candidate)

    return candidates

# =========================
# BUILD PROMPT
# =========================

def build_prompt(
    place_clean,
    parts,
    candidates
):

    return [
        {
            "role": "system",
            "content": """
Eres un experto en geografía administrativa de Galicia.

Tu trabajo es mapear lugares ambiguos a:
- lugar
- parroquia
- concello
- provincia

Debes:
- usar SOLO los candidatos proporcionados
- NO inventar datos
- devolver SOLO JSON válido
- elegir el candidato más coherente

Devuelve:

{
  "matched": true,
  "confidence": 0-100,
  "selected_candidate": {
    "lugar": "...",
    "parroquia": "...",
    "concello": "...",
    "provincia": "..."
  },
  "reason": "..."
}

Si no hay suficiente confianza:

{
  "matched": false,
  "confidence": 0,
  "reason": "..."
}
"""
        },
        {
            "role": "user",
            "content": json.dumps(
                {
                    "input_place": place_clean,
                    "parts": parts,
                    "candidates": candidates
                },
                ensure_ascii=False,
                indent=2
            )
        }
    ]

# =========================
# OLLAMA CALL
# =========================

def call_ollama(messages):

    response = requests.post(
        OLLAMA_URL,
        json={
            "model": OLLAMA_MODEL,
            "temperature": 0,
            "stream": False,
            "messages": messages
        }
    )

    response.raise_for_status()

    data = response.json()

    content = data["message"]["content"]

    return json.loads(content)

# =========================
# MATCH ONE PLACE
# =========================

def match_place(
    place_clean,
    parts
):

    candidates = retrieve_candidates(
        place_clean
    )

    prompt = build_prompt(
        place_clean,
        parts,
        candidates
    )

    result = call_ollama(
        prompt
    )

    return result

# =========================
# BATCH PIPELINE
# =========================

def run_pipeline(input_csv, output_csv):

    df = pd.read_csv(input_csv)

    results = []

    for row in df.itertuples():

        place_clean = row.place_clean

        if pd.isna(place_clean):

            continue

        parts = []

        for i in range(1, 7):

            col = f"place_part{i}"

            if hasattr(row, col):

                value = getattr(row, col)

                if pd.notna(value):

                    parts.append(value)

        print("\n====================")
        print("PLACE:", place_clean)

        try:

            result = match_place(
                place_clean,
                parts
            )

            print(result)

            output = {
                "place_clean": place_clean,
                "matched": result.get("matched"),
                "confidence": result.get("confidence"),
                "reason": result.get("reason")
            }

            selected = result.get(
                "selected_candidate",
                {}
            )

            output["canonical_lugar"] = (
                selected.get("lugar")
            )

            output["canonical_parroquia"] = (
                selected.get("parroquia")
            )

            output["canonical_concello"] = (
                selected.get("concello")
            )

            output["canonical_provincia"] = (
                selected.get("provincia")
            )

            results.append(output)

        except Exception as e:

            print("❌ ERROR:", e)

            results.append({
                "place_clean": place_clean,
                "matched": False,
                "confidence": 0,
                "reason": str(e)
            })

    out_df = pd.DataFrame(results)

    out_df.to_csv(
        output_csv,
        index=False
    )

    print("\n✅ Matching completed")

# =========================
# ENTRYPOINT
# =========================

if __name__ == "__main__":

    INPUT = (
        AUX_DIR / "places_normalized.csv"
    )

    OUTPUT = (
        AUX_DIR / "places_matched.csv"
    )

    run_pipeline(
        INPUT,
        OUTPUT
    )