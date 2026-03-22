from pathlib import Path
from ged4py.parser import GedcomReader

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_PATH = BASE_DIR / "data/raw/chamadoira_2025.ged"

with GedcomReader(str(RAW_PATH)) as parser:
    for idx, fam in enumerate(parser.records0("FAM")):
        print(f"\n================ FAMILIA {idx+1} =================")
        print(f"ID: {fam.xref_id}, tag={fam.tag}, subrecords={len(fam.sub_records)}, subtags={len(fam.sub_tags())}")

        # listar sub_tags (nivel inmediato)
        print("→ Sub-tags:")
        for st in fam.sub_tags():
            print(f"   {st.tag:<10s} value={st.value}")

        # listar sub_records si existen
        if fam.sub_records:
            print("→ Sub-records:")
            for r in fam.sub_records:
                print(f"   tag={r.tag}, xref={r.xref_id}, value={r.value}, sub_tags={len(r.sub_tags())}")
                # mostrar sub-tags dentro de ese sub-record (nivel más profundo)
                for st in r.sub_tags():
                    print(f"      ▪ {st.tag:<10s} value={st.value}")

        # corta después de 2 familias solo para no saturar
        if idx == 1:
            break
