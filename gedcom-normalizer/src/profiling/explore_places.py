from pathlib import Path
from ged4py.parser import GedcomReader
from collections import Counter, defaultdict

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_PATH = BASE_DIR / "data/raw/chamadoira_2026.ged"

# contadores
place_tag_counts = Counter()          # (parent_tag, event_tag)
place_values = Counter()              # valores de lugares
place_examples = defaultdict(list)    # ejemplos por tipo

with GedcomReader(str(RAW_PATH)) as parser:
    for rec in parser.records0():

        # recorrer subtags (BIRT, DEAT, MARR, etc.)
        for sub in rec.sub_tags():

            # buscar PLAC dentro de cada evento
            for subsub in sub.sub_tags():
                if subsub.tag == "PLAC":
                    key = (rec.tag, sub.tag)
                    place_tag_counts[key] += 1

                    value = subsub.value
                    if value:
                        value = str(value).strip()

                        place_values[value] += 1

                        # guardar algunos ejemplos (máx 5 por tipo)
                        if len(place_examples[key]) < 5:
                            place_examples[key].append(value)

# =========================
# RESULTADOS
# =========================

print("\n=== 📍 Dónde aparecen lugares ===")
for (parent, event), count in place_tag_counts.most_common():
    print(f"{parent} -> {event:10s} {count}")

print("\n=== 🧾 Ejemplos de lugares por tipo ===")
for (parent, event), examples in place_examples.items():
    print(f"\n{parent} -> {event}")
    for ex in examples:
        print(f"  - {ex}")

print("\n=== 🔢 Lugares más comunes ===")
for place, count in place_values.most_common(20):
    print(f"{place[:60]:60s} {count}")

print("\n✅ Exploración de lugares completada.")