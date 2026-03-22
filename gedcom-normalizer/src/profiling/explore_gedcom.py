from pathlib import Path
from ged4py.parser import GedcomReader
from collections import Counter

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_PATH = BASE_DIR / "data/raw/chamadoira_2025.ged"

tag_counts = Counter()
subtag_counts = Counter()

with GedcomReader(str(RAW_PATH)) as parser:
    # cuántos registros de cada tipo hay a nivel 0
    for rec in parser.records0():
        tag_counts[rec.tag] += 1

        # contamos subtags dentro de FAM y INDI
        if rec.tag in ("FAM", "INDI"):
            for sub in rec.sub_tags():
                subtag_counts[(rec.tag, sub.tag)] += 1

# imprimir resumen general
print("\n=== Tipos de registros (nivel 0) ===")
for tag, count in tag_counts.most_common():
    print(f"{tag:10s} {count}")

print("\n=== Subtags más comunes en FAM ===")
for (parent, sub), count in subtag_counts.most_common():
    if parent == "FAM":
        print(f"  {sub:10s} {count}")

print("\n=== Subtags más comunes en INDI ===")
for (parent, sub), count in subtag_counts.most_common():
    if parent == "INDI":
        print(f"  {sub:10s} {count}")

print("\n✅ Exploración completada.")
