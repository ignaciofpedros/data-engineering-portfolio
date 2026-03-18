# 🧬 GEDCOM Normalizer

Proyecto de ingeniería de datos que procesa archivos **GEDCOM** (formato de genealogía) 
y los transforma en un modelo tabular limpio, con control de calidad mediante *tests*.

---

## 🎯 Objetivo

Convertir archivos `.ged` en datos estructurados y validados, listos para análisis o visualización.

---

## ⚙️ Pipeline general

1. **Ingesta (Python)**
   - Lee archivos `.ged` usando [`ged4py`](https://pypi.org/project/ged4py/).
   - Extrae entidades: personas, familias, eventos.
   - Exporta a CSV o Parquet.

2. **Transformación (dbt)**
   - Modelos de *staging* y *mart*.
   - Limpieza de fechas, claves únicas, relaciones normalizadas.

3. **Tests**
   - `dbt tests` o `Great Expectations` para verificar:
     - Campos obligatorios (nombre, fecha nacimiento).
     - Fechas válidas.
     - IDs únicos.
     - Integridad de relaciones familia–persona.

4. **Analítica**
   - Notebook exploratorio (edad promedio, generaciones, etc.)
   - Diagramas de relación.

---

## 🧩 Estructura

