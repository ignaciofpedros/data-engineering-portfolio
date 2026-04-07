{{ config(materialized='view') }}
SELECT DISTINCT
    date_raw
FROM read_csv_auto('../../../data/staging/dates.csv')