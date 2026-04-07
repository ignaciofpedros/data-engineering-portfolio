{{ config(materialized='view') }}

SELECT
    palabra_clave AS key_word,
    id AS place_id,
    prioridad AS priority 
FROM {{ ref('geo_ref') }} place
