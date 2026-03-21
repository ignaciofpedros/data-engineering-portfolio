from pathlib import Path
from ged4py.parser import GedcomReader
import pandas as pd

# Paths
BASE_DIR = Path(__file__).resolve().parents[2]
RAW_PATH = BASE_DIR / "data/raw/chamadoira_2025.ged"
STAGING_DIR = BASE_DIR / "data/staging"
STAGING_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH = STAGING_DIR / "people.csv"

with GedcomReader(str(RAW_PATH)) as parser:
    people = []
    for ind in parser.records0("INDI"):
        # buscar birth
        birth_tag = ind.sub_tag("BIRT")
        birth_date_tag = birth_tag.sub_tag("DATE") if birth_tag else None

        person = {
            "id": ind.xref_id,
            "name": ind.name.format() if ind.name else None,
            "birth": str(birth_date_tag.value) if birth_date_tag else None
        }
        people.append(person)

df = pd.DataFrame(people)
df.to_csv(OUTPUT_PATH, index=False)

print(f"✅ Staging generado en: {OUTPUT_PATH}")