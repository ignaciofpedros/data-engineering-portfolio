{{ config(materialized='table') }}

SELECT
    death_id,
    id AS person_id,
    child.family_id,
    name,
    death_precision,
    death_place
FROM {{ ref('int_people') }} people
LEFT JOIN {{ ref('stg_family_children') }} child
    ON people.id = child.child_id