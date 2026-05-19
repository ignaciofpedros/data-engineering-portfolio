{{ config(materialized='table') }}

SELECT
    birth_id,
    id AS person_id,
    child.family_id,
    name,
    birth_precision,
    birth_place
FROM {{ ref('int_people') }} people
LEFT JOIN {{ ref('stg_family_children') }} child
    ON people.id = child.child_id