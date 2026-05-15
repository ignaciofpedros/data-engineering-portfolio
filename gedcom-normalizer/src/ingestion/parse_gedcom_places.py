from pathlib import Path
from ged4py.parser import GedcomReader
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_PATH = BASE_DIR / "data/raw/chamadoira_2026.ged"
STAGING_DIR = BASE_DIR / "data/staging"
STAGING_DIR.mkdir(parents=True, exist_ok=True)

places_output = STAGING_DIR / "places.csv"

rows = []

with GedcomReader(str(RAW_PATH)) as parser:
    for rec in parser.records0():

        entity_type = rec.tag      # INDI, FAM, etc.
        entity_id = rec.xref_id

        # recorrer eventos (BIRT, DEAT, MARR, EVEN, etc.)
        for sub in rec.sub_records:

            event_type = sub.tag

            # buscar PLAC dentro del evento
            place_tag = sub.sub_tag("PLAC")
            if place_tag and place_tag.value:

                rows.append({
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "event_type": event_type,
                    "place_raw": str(place_tag.value).strip()
                })

# exportar
df = pd.DataFrame(rows)
df.to_csv(places_output, index=False)

print("✅ Lugares extraídos correctamente:")
print(f" - Output: {places_output}")
print(f"Total lugares: {len(df)}")