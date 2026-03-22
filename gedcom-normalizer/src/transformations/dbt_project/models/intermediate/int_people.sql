{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_people') }}
),

-- 1️⃣ Limpieza
cleaned AS (
    SELECT
        id,
        name,
        birth_raw,
        REGEXP_REPLACE(birth_raw, '^\((.*)\)$', '\1') AS birth_clean,
        LOWER(REGEXP_REPLACE(birth_raw, '^\((.*)\)$', '\1')) AS birth_lower
    FROM source
),

-- 2️⃣ Clasificación
classified AS (
    SELECT
        *,
        CASE
            WHEN birth_clean ~ '^[0-9]{1,2} \S+ [0-9]{4}$' THEN 'full_date'
            WHEN birth_clean ~ '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$' THEN 'date_dash'
            WHEN birth_clean ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN 'date_slash'
            WHEN birth_clean ~ '^.+ [0-9]{4}$' THEN 'text_and_year'
            WHEN birth_clean ~ '^[0-9]{4}$' THEN 'year_only'
            WHEN birth_clean IS NULL OR birth_clean = '' THEN 'null'
            ELSE 'other'
        END AS birth_type
    FROM cleaned
),

-- 3️⃣ Parsing enriquecido
parsed AS (
    SELECT
        id,
        name,
        birth_raw,
        birth_clean,
        birth_type,

        -- 🎯 AÑO
        CAST(NULLIF(REGEXP_EXTRACT(birth_clean, '([0-9]{4})'), '') AS INTEGER) AS birth_year,


        -- 🎯 MES (solo si se puede inferir)
        CASE
            WHEN birth_type = 'date_dash'
                THEN CAST(NULLIF(SPLIT_PART(birth_clean, '-', 2), '') AS INTEGER)

            WHEN birth_type = 'date_slash'
                THEN CAST(NULLIF(SPLIT_PART(birth_clean, '/', 2), '') AS INTEGER)

            WHEN birth_type = 'text_and_year' OR birth_type = 'full_date' THEN
                CASE
                    WHEN birth_lower LIKE '%ene%' THEN 1
                    WHEN birth_lower LIKE '%feb%' THEN 2
                    WHEN birth_lower LIKE '%mar%' THEN 3
                    WHEN birth_lower LIKE '%abr%' THEN 4
                    WHEN birth_lower LIKE '%may%' THEN 5
                    WHEN birth_lower LIKE '%jun%' THEN 6
                    WHEN birth_lower LIKE '%jul%' THEN 7
                    WHEN birth_lower LIKE '%ago%' THEN 8
                    WHEN birth_lower LIKE '%sep%' OR birth_lower LIKE '%set%' THEN 9
                    WHEN birth_lower LIKE '%oct%' THEN 10
                    WHEN birth_lower LIKE '%nov%' THEN 11
                    WHEN birth_lower LIKE '%dic%' THEN 12
                    ELSE NULL
                END

            ELSE NULL
        END AS birth_month,

        -- 🎯 DÍA
        CASE
            WHEN birth_type IN ('date_dash', 'date_slash')
                THEN CAST(SPLIT_PART(birth_clean, CASE WHEN birth_type = 'date_dash' THEN '-' ELSE '/' END, 1) AS INTEGER)

            WHEN birth_type = 'full_date'
                THEN TRY_CAST(REGEXP_EXTRACT(birth_clean, '^([0-9]{1,2})') AS INTEGER)

            ELSE NULL
        END AS birth_day

    FROM classified
),

-- 4️⃣ Construcción final
final AS (
    SELECT
        *,
        
        -- 🎯 FECHA FINAL
        CASE
            WHEN birth_year IS NULL THEN NULL
            ELSE MAKE_DATE(
                birth_year,
                COALESCE(birth_month, 1),
                COALESCE(birth_day, 1)
            )
        END AS birth_date,

        -- 🎯 PRECISIÓN 🔥
        CASE
            WHEN birth_day IS NOT NULL THEN 'day'
            WHEN birth_month IS NOT NULL THEN 'month'
            WHEN birth_year IS NOT NULL THEN 'year'
            ELSE 'unknown'
        END AS birth_precision

    FROM parsed
)

SELECT
    id,
    name,
    birth_raw,
    birth_clean,
    birth_type,
    birth_year,
    birth_month,
    birth_day,
    birth_date,
    birth_precision
FROM final