{{ config(materialized='view') }}

SELECT
    palabra_clave AS key_word,
    id AS id_place,
    tipo AS type,
    municipio AS city,
    provincia AS province 
FROM {{ ref('geo_ref') }} place
