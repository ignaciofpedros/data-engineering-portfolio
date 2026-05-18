{{ config(materialized='view') }}
SELECT DISTINCT
    entity_type as type_id,
    entity_id   as id,
    event_type  as event_type,
    place_raw   as place_name
FROM read_csv_auto('../../../data/staging/places.csv')