import os
from ged4py.parser import GedcomReader
import pandas as pd

# ruta del archivo fuente
gedcom_path = os.path.join(os.path.dirname(__file__), "data/chamadoira_2025.ged")

# ruta para la salida
output_dir = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(output_dir, exist_ok=True)  # crea 'data/' si no existe
output_path = os.path.join(output_dir, "people.csv")

with GedcomReader(gedcom_path) as parser:
    people = [
        {
            "id": ind.xref_id,
            "name": ind.name.format(),
            "birth": str(ind.sub_tag("BIRT.DATE").value) if ind.sub_tag("BIRT.DATE") else None
        }
        for ind in parser.records0("INDI")
    ]

df = pd.DataFrame(people)

df.to_csv(output_path, index=False)
print(f"✅ Archivo CSV guardado en: {output_path}")
