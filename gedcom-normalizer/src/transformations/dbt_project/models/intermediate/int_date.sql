{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_date') }}
),

-- 1️⃣ Limpieza
cleaned AS (
    SELECT
        date_raw,
        REGEXP_REPLACE(date_raw, '^\((.*)\)$', '\1') AS date_clean,
        LOWER(REGEXP_REPLACE(date_raw, '^\((.*)\)$', '\1')) AS date_lower
    FROM source
),

-- 2️⃣ Clasificación
classified AS (
    SELECT
        *,
        CASE
            WHEN date_clean ~ '^[0-9]{1,2} \S+ [0-9]{4}$' THEN 'full_date'
            WHEN date_clean ~ '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$' THEN 'date_dash'
            WHEN date_clean ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN 'date_slash'
            WHEN date_clean ~ '^.+ [0-9]{4}$' THEN 'text_and_year'
            WHEN date_clean ~ '^[0-9]{4}$' THEN 'year_only'
            WHEN date_clean IS NULL OR date_clean = '' THEN 'null'
            ELSE 'other'
        END AS date_type
    FROM cleaned
),

-- 3️⃣ Parsing enriquecido
parsed AS (
    SELECT
        date_raw,
        date_clean,
        date_type,

        -- 🎯 AÑO
        CAST(NULLIF(REGEXP_EXTRACT(date_clean, '([0-9]{4})'), '') AS INTEGER) AS date_year,


        -- 🎯 MES (solo si se puede inferir)
        CASE
            WHEN date_type = 'date_dash'
                THEN CAST(NULLIF(SPLIT_PART(date_clean, '-', 2), '') AS INTEGER)

            WHEN date_type = 'date_slash'
                THEN CAST(NULLIF(SPLIT_PART(date_clean, '/', 2), '') AS INTEGER)

            WHEN date_type = 'text_and_year' OR date_type = 'full_date' THEN
                CASE
                    WHEN date_lower LIKE '%ene%' THEN 1
                    WHEN date_lower LIKE '%feb%' THEN 2
                    WHEN date_lower LIKE '%mar%' THEN 3
                    WHEN date_lower LIKE '%abr%' THEN 4
                    WHEN date_lower LIKE '%may%' THEN 5
                    WHEN date_lower LIKE '%jun%' THEN 6
                    WHEN date_lower LIKE '%jul%' THEN 7
                    WHEN date_lower LIKE '%ago%' THEN 8
                    WHEN date_lower LIKE '%sep%' OR date_lower LIKE '%set%' THEN 9
                    WHEN date_lower LIKE '%oct%' THEN 10
                    WHEN date_lower LIKE '%nov%' THEN 11
                    WHEN date_lower LIKE '%dic%' THEN 12
                    ELSE NULL
                END

            ELSE NULL
        END AS date_month,

        -- 🎯 DÍA
        CASE
            WHEN date_type IN ('date_dash', 'date_slash')
                THEN CAST(SPLIT_PART(date_clean, CASE WHEN date_type = 'date_dash' THEN '-' ELSE '/' END, 1) AS INTEGER)

            WHEN date_type = 'full_date'
                THEN TRY_CAST(REGEXP_EXTRACT(date_clean, '^([0-9]{1,2})') AS INTEGER)

            ELSE NULL
        END AS date_day

    FROM classified
),

-- 4️⃣ Construcción final
final AS (
    SELECT
        *,
        
        -- 🎯 FECHA FINAL
        CASE
            WHEN date_year IS NULL THEN NULL
            ELSE TRY_CAST(
                date_year || '-' ||
                COALESCE(date_month, 1) || '-' ||
                COALESCE(date_day, 1) AS DATE
            )
        END AS date_id,

        -- 🎯 PRECISIÓN 🔥
        CASE
            WHEN date_day IS NOT NULL THEN 'day'
            WHEN date_month IS NOT NULL THEN 'month'
            WHEN date_year IS NOT NULL THEN 'year'
            ELSE 'unknown'
        END AS date_precision

    FROM parsed
)

SELECT
    date_raw,
    date_clean,
    date_type,
    date_year,
    date_month,
    date_day,
    date_id,
    date_precision
FROM final