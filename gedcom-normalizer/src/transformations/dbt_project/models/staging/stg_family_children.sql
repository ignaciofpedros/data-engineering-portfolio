{{ config(materialized='view') }}

SELECT
    family_id,
    child_id
FROM read_csv_auto('../../../data/staging/family_children.csv')
