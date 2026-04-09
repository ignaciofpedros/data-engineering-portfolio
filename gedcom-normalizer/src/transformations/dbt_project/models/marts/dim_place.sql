{{ config(materialized='table') }}

SELECT
    id AS place_id,
    nombre AS place_name,
    tipo AS place_type, 
    municipio AS place_city, 
    provincia AS place_province 
FROM {{ ref('places') }} place
