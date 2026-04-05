from pathlib import Path
from ged4py.parser import GedcomReader
from collections import Counter, defaultdict

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_PATH = BASE_DIR / "data/raw/chamadoira_2025.ged"

# contadores
date_tag_counts = Counter()          # (parent_tag, event_tag)
date_values = Counter()              # valores de fechas
date_examples = defaultdict(list)    # ejemplos por tipo

with GedcomReader(str(RAW_PATH)) as parser:
    for rec in parser.records0():

        # recorrer subtags (eventos tipo BIRT, MARR, etc.)
        for sub in rec.sub_tags():

            # buscar DATE dentro de cada evento
            for subsub in sub.sub_tags():
                if subsub.tag == "DATE":
                    key = (rec.tag, sub.tag)
                    date_tag_counts[key] += 1

                    value = subsub.value
                    if value:
                        date_values[value] += 1

                        # guardar algunos ejemplos (máx 5 por tipo)
                        if len(date_examples[key]) < 5:
                            date_examples[key].append(value)

# =========================
# RESULTADOS
# =========================

print("\n=== 📅 Dónde aparecen fechas ===")
for (parent, event), count in date_tag_counts.most_common():
    print(f"{parent} -> {event:10s} {count}")

print("\n=== 🧾 Ejemplos de fechas por tipo ===")
for (parent, event), examples in date_examples.items():
    print(f"\n{parent} -> {event}")
    for ex in examples:
        print(f"  - {ex}")

print("\n=== 🔢 Fechas más comunes ===")
for date, count in date_values.most_common(10):
    print(f"{str(date):15s} {count}")

print("\n✅ Exploración de fechas completada.")