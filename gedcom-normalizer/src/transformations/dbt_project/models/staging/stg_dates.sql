{{ config(materialized='view') }}
SELECT
    date_raw
FROM read_csv_auto('../../../data/staging/dates.csv')