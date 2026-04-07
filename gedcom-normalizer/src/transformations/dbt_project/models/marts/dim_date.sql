{{ config(materialized='view') }}

SELECT
    date_raw,
    date_clean,
    date_type,
    date_year,
    date_month,
    date_day,
    TRY_CAST(date_id AS DATE) AS date_id,
    date_precision
FROM {{ ref('int_date') }} dates
