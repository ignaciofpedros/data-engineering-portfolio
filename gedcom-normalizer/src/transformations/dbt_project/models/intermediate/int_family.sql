{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_family') }}
),

-- 1️⃣ Limpieza
cleaned AS (
    SELECT
        s.family_id,
        s.husband_id,
        s.wife_id,
        COALESCE(d.date_id,DATE('9999-12-31')) AS marriage_date_id,
        COALESCE(d.date_precision, '-') AS date_precision,
        COALESCE(p.place_id, -1) AS marriage_place_id
    FROM source s
    LEFT JOIN {{ ref('dim_date') }} d
        ON s.marriage_date_raw = d.date_raw
    LEFT JOIN {{ ref('dim_place_alias') }} p
        ON s.marriage_place ILIKE '%' || p.key_word || '%'
    QUALIFY ROW_NUMBER() OVER ( PARTITION BY p.place_id ORDER BY p.priority ASC) = 1
)

SELECT
    family_id,
    husband_id,
    wife_id,
    marriage_date_id,
    date_precision,
    marriage_place_id
FROM cleaned