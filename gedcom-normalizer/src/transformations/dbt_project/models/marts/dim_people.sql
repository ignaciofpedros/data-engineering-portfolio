{{ config(materialized='table') }}

SELECT
    id AS person_id,
    child.family_id,
    name,
    birth_id,
    birth_precision
FROM {{ ref('int_people') }} people
LEFT JOIN {{ ref('stg_family_children') }} child
    ON people.id = child.child_id