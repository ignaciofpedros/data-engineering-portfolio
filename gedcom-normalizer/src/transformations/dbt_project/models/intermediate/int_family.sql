{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_families') }}
),

-- 1️⃣ Limpieza
cleaned AS (
    SELECT
        s.family_id,
        s.husband_id,
        s.wife_id,
        COALESCE(d.id_date,DATE('9999-12-31')) AS id_date,
        COALESCE(d.date_precision, '-') AS date_precision,
        COALESCE(p.id_place, '-') AS id_place
    FROM source s
    LEFT JOIN {{ ref('dim_date') }} d
        ON s.marriage_date_raw = d.date_raw
    LEFT JOIN {{ ref('dim_place') }} p
        ON s.marriage_place LIKE '%' || p.key_word || '%'
)

SELECT
    family_id,
    husband_id,
    wife_id,
    id_date,
    date_precision,
    id_place
FROM cleaned