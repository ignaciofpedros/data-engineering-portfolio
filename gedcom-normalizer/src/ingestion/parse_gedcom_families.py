from pathlib import Path
from ged4py.parser import GedcomReader
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_PATH = BASE_DIR / "data/raw/chamadoira_2025.ged"
STAGING_DIR = BASE_DIR / "data/staging"
STAGING_DIR.mkdir(parents=True, exist_ok=True)

families_output = STAGING_DIR / "families.csv"
children_output = STAGING_DIR / "family_children.csv"

families, children = [], []

with GedcomReader(str(RAW_PATH)) as parser:
    for fam in parser.records0("FAM"):
        fam_id = fam.xref_id

        # inicializar variables
        husband_id = wife_id = marriage_date = marriage_place = None

        # recorrer sub_records dentro de cada familia
        for sub in fam.sub_records:
            tag = sub.tag
            if tag == "HUSB":
                husband_id = sub.value
            elif tag == "WIFE":
                wife_id = sub.value
            elif tag == "CHIL":
                children.append({"family_id": fam_id, "child_id": sub.value})
            elif tag == "MARR":
                date_tag = sub.sub_tag("DATE")
                place_tag = sub.sub_tag("PLAC")
                marriage_date = date_tag.value if date_tag else None
                marriage_place = place_tag.value if place_tag else None

        families.append({
            "family_id": fam_id,
            "husband_id": husband_id,
            "wife_id": wife_id,
            "marriage_date_raw": marriage_date,
            "marriage_place": marriage_place
        })

# exportar resultados
pd.DataFrame(families).to_csv(families_output, index=False)
pd.DataFrame(children).to_csv(children_output, index=False)

print("✅ Familias extraídas correctamente:")
print(f" - Families: {families_output}")
print(f" - Family children: {children_output}")
print(f"Total familias: {len(families)}, hijos: {len(children)}")
