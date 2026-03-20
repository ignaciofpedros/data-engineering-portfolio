{{ config(materialized='view') }}

WITH remove_parenthesis as (
    SELECT
        CASE
            WHEN birth_raw LIKE '(%' AND birth_raw LIKE '%)'
                THEN SUBSTR(birth_raw, 2, LENGTH(birth_raw) - 2)
            ELSE birth_raw
        END AS birth_clean
    FROM stg_people
), classified AS (
    SELECT
        CASE
            -- Fecha completa tipo "15 APR 1920" o "12 Septiembre 1910"
            WHEN birth_clean ~ '^[0-9]{1,2} \S+ [0-9]{4}$' THEN 'full_date_flexible'
            -- Fecha tipo "15-04-1920"
            WHEN birth_clean ~ '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$' THEN 'date_dash'
            -- Fecha tipo "15/04/1920"
            WHEN birth_clean ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN 'date_slash'
            -- Cualquier cadena + año tipo "circa 1920"
            WHEN birth_clean ~ '^.+ [0-9]{4}$' THEN 'text_and_year'
            -- Solo año tipo "1920"
            WHEN birth_clean ~ '^[0-9]{4}$' THEN 'year_only'
            -- Nulo o vacío
            WHEN birth_clean IS NULL OR birth_clean = '' THEN 'null'
            -- Todo lo demás
            ELSE 'other'
        END AS type
    FROM remove_parenthesis
)
SELECT type, COUNT(*) AS cnt
FROM classified
GROUP BY type
ORDER BY cnt DESC