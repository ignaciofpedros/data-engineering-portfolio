import re

import pandas as pd
import unidecode


def normalize_text(text: str) -> str:

    """
    Normalize geographic place names.

    Steps:
    - lowercase
    - remove accents
    - normalize separators
    - remove strange characters
    - normalize whitespace
    """

    # =========================
    # NULL CHECK
    # =========================

    if pd.isna(text) or text is None:
        return None

    # =========================
    # STRING
    # =========================

    text = str(text).strip().lower()

    if not text:
        return None

    # =========================
    # REMOVE ACCENTS
    # =========================

    text = unidecode.unidecode(text)

    # =========================
    # NORMALIZE SEPARATORS
    # =========================

    text = text.replace(";", ",")
    text = text.replace("/", ",")

    # =========================
    # REMOVE STRANGE CHARS
    # =========================

    text = re.sub(
        r"[^\w\s,.-]",
        " ",
        text
    )

    # =========================
    # NORMALIZE WHITESPACE
    # =========================

    text = re.sub(
        r"\s+",
        " ",
        text
    )

    # =========================
    # NORMALIZE COMMAS
    # =========================

    text = re.sub(
        r"\s*,\s*",
        ", ",
        text
    )

    text = re.sub(
        r",+",
        ",",
        text
    )

    # =========================
    # CLEAN EDGES
    # =========================

    text = text.strip(" ,.-")

    # =========================
    # EMPTY CHECK
    # =========================

    if not text:
        return None

    return text