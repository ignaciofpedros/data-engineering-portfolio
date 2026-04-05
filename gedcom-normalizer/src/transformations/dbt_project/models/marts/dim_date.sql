{{ config(materialized='view') }}

SELECT
    date_raw,
    date_clean,
    date_type,
    date_year,
    date_month,
    date_day,
    TRY_CAST(id_date AS DATE) AS id_date,
    date_precision
FROM {{ ref('int_dates') }} dates
