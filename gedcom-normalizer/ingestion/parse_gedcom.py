from ged4py.parser import GedcomReader
import pandas as pd

path = "data/chamadoira_2025.ged"

with GedcomReader(path) as parser:
    people = [
        {"id": ind.xref_id,
         "name": ind.name.format(),
         "birth": str(ind.sub_tag("BIRT.DATE").value) if ind.sub_tag("BIRT.DATE") else None}
        for ind in parser.records0("INDI")
    ]

df = pd.DataFrame(people)
df.to_csv("data_people.csv", index=False)
print(f"Exportadas {len(df)} personas ✅")
