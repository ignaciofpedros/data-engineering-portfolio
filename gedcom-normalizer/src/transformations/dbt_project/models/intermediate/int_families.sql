{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_families') }}
),

-- 1️⃣ Limpieza
cleaned AS (
    SELECT
        family_id,
        husband_id,
        wife_id,
        marriage_date_raw,
        REGEXP_REPLACE(marriage_date_raw, '^\((.*)\)$', '\1') AS marriage_date_clean,
        LOWER(REGEXP_REPLACE(marriage_date_raw, '^\((.*)\)$', '\1')) AS marriage_date_lower
    FROM source
),

-- 2️⃣ Clasificación
classified AS (
    SELECT
        *,
        CASE
            WHEN marriage_date_clean ~ '^[0-9]{1,2} \S+ [0-9]{4}$' THEN 'full_date'
            WHEN marriage_date_clean ~ '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$' THEN 'date_dash'
            WHEN marriage_date_clean ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN 'date_slash'
            WHEN marriage_date_clean ~ '^.+ [0-9]{4}$' THEN 'text_and_year'
            WHEN marriage_date_clean ~ '^[0-9]{4}$' THEN 'year_only'
            WHEN marriage_date_clean IS NULL OR marriage_date_clean = '' THEN 'null'
            ELSE 'other'
        END AS marriage_date_type
    FROM cleaned
),

-- 3️⃣ Parsing enriquecido
parsed AS (
    SELECT
        family_id,
        husband_id,
        wife_id,
        marriage_date_raw,
        marriage_date_clean,
        marriage_date_type,

        -- 🎯 AÑO
        CAST(REGEXP_EXTRACT(marriage_date_clean, '([0-9]{4})') AS INTEGER) AS marriage_date_year,

        -- 🎯 MES (solo si se puede inferir)
        CASE
            WHEN marriage_date_type = 'date_dash'
                THEN CAST(SPLIT_PART(marriage_date_clean, '-', 2) AS INTEGER)

            WHEN marriage_date_type = 'date_slash'
                THEN CAST(SPLIT_PART(marriage_date_clean, '/', 2) AS INTEGER)

            WHEN marriage_date_type = 'full_date'
                THEN EXTRACT(MONTH FROM TRY_STRPTIME(marriage_date_clean, '%d %b %Y'))

            WHEN marriage_date_type = 'text_and_year' THEN
                CASE
                    WHEN marriage_date_lower LIKE '%ene%' THEN 1
                    WHEN marriage_date_lower LIKE '%feb%' THEN 2
                    WHEN marriage_date_lower LIKE '%mar%' THEN 3
                    WHEN marriage_date_lower LIKE '%abr%' THEN 4
                    WHEN marriage_date_lower LIKE '%may%' THEN 5
                    WHEN marriage_date_lower LIKE '%jun%' THEN 6
                    WHEN marriage_date_lower LIKE '%jul%' THEN 7
                    WHEN marriage_date_lower LIKE '%ago%' THEN 8
                    WHEN marriage_date_lower LIKE '%sep%' OR marriage_date_lower LIKE '%set%' THEN 9
                    WHEN marriage_date_lower LIKE '%oct%' THEN 10
                    WHEN marriage_date_lower LIKE '%nov%' THEN 11
                    WHEN marriage_date_lower LIKE '%dic%' THEN 12
                    ELSE NULL
                END

            ELSE NULL
        END AS marriage_date_month,

        -- 🎯 DÍA
        CASE
            WHEN marriage_date_type IN ('date_dash', 'date_slash')
                THEN CAST(SPLIT_PART(marriage_date_clean, CASE WHEN marriage_date_type = 'date_dash' THEN '-' ELSE '/' END, 1) AS INTEGER)

            WHEN marriage_date_type = 'full_date'
                THEN EXTRACT(DAY FROM TRY_STRPTIME(marriage_date_clean, '%d %b %Y'))

            ELSE NULL
        END AS marriage_date_day

    FROM classified
),

-- 4️⃣ Construcción final
final AS (
    SELECT
        *,
        
        -- 🎯 FECHA FINAL
        CASE
            WHEN marriage_date_year IS NULL THEN NULL
            ELSE MAKE_DATE(
                marriage_date_year,
                COALESCE(marriage_date_month, 1),
                COALESCE(marriage_date_day, 1)
            )
        END AS marriage_date,

        -- 🎯 PRECISIÓN 🔥
        CASE
            WHEN marriage_date_day IS NOT NULL THEN 'day'
            WHEN marriage_date_month IS NOT NULL THEN 'month'
            WHEN marriage_date_year IS NOT NULL THEN 'year'
            ELSE 'unknown'
        END AS marriage_date_precision

    FROM parsed
)

SELECT
    family_id,
    husband_id,
    wife_id,
    marriage_date_raw,
    marriage_date_clean,
    marriage_date_type,
    marriage_date_year,
    marriage_date_month,
    marriage_date_day,
    marriage_date,
    marriage_date_precision
FROM final