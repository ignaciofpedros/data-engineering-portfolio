{{ config(materialized='ephemeral') }}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_people') }}
),

-- 1️⃣ Limpieza
cleaned AS (
    SELECT
        id,
        name,
        COALESCE(d.date_id,DATE('9999-12-31')) AS birth_id,
        COALESCE(d.date_precision, '-') AS birth_precision,
    FROM source s
    LEFT JOIN {{ ref('dim_date') }} d
        ON s.birth_raw = d.date_raw
)

SELECT
    id,
    name,
    birth_id,
    birth_precision
FROM cleaned