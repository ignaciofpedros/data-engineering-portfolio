from pathlib import Path
from ged4py.parser import GedcomReader
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_PATH = BASE_DIR / "data/raw/chamadoira_2025.ged"
STAGING_DIR = BASE_DIR / "data/staging"
STAGING_DIR.mkdir(parents=True, exist_ok=True)

dates_output = STAGING_DIR / "dates.csv"

rows = []

with GedcomReader(str(RAW_PATH)) as parser:
    for rec in parser.records0():

        entity_type = rec.tag      # INDI, FAM, etc.
        entity_id = rec.xref_id

        # recorrer eventos (BIRT, DEAT, MARR, EVEN, etc.)
        for sub in rec.sub_records:

            event_type = sub.tag

            # buscar DATE dentro del evento
            date_tag = sub.sub_tag("DATE")
            if date_tag and date_tag.value:

                rows.append({
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "event_type": event_type,
                    "date_raw": str(date_tag.value)  # 🔑 clave: convertir a string
                })

# exportar
df = pd.DataFrame(rows)
df.to_csv(dates_output, index=False)

print("✅ Fechas extraídas correctamente:")
print(f" - Output: {dates_output}")
print(f"Total fechas: {len(df)}")