{{ config(materialized='view') }}
SELECT
    family_id,
    husband_id,
    wife_id,
    TRY_CAST(marriage_date_raw AS TEXT) AS marriage_date_raw,
    marriage_place
FROM read_csv_auto('../../../data/staging/families.csv')