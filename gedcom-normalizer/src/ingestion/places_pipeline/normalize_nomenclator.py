from pathlib import Path
import pandas as pd
import re

BASE_DIR = Path(__file__).resolve().parents[3]
STAGING_DIR = BASE_DIR / "data/staging"
AUX_DIR = STAGING_DIR / "places_aux"

input_data = AUX_DIR / "nomenclator.csv"
output = AUX_DIR / "nomenclator_transformado.csv"

df = pd.read_csv(input_data, sep=";")

articles = {"O", "Os", "A", "As"}

cols = df.columns.tolist()

# recorrer de derecha a izquierda (N → A)
for i in range(len(cols) - 1, 0, -1):
    col_cur = cols[i]
    col_prev = cols[i - 1]

    def merge(row):
        cur = str(row[col_cur]).strip() if pd.notna(row[col_cur]) else ""
        prev = str(row[col_prev]).strip() if pd.notna(row[col_prev]) else ""

        if not cur:
            return pd.Series([prev, cur])

        # CASO 1: solo artículo
        if cur in articles:
            if prev:
                new_prev = f"{cur} {prev}"
            else:
                new_prev = cur
            return pd.Series([new_prev, ""])

        # CASO 2: artículo + "ou" o "-"
        m = re.match(r"^(O|Os|A|As)\s*(ou|-)\s*(.+)$", cur)
        if m:
            art, sep, rest = m.groups()
            if prev:
                new_prev = f"{art} {prev} {sep} {rest}"
            else:
                new_prev = f"{art} {rest}"
            return pd.Series([new_prev, ""])

        return pd.Series([prev, cur])

    df[[col_prev, col_cur]] = df.apply(merge, axis=1)

df.to_csv(output, index=False)