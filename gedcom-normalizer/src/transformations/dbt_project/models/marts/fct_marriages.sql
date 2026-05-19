{{ config(materialized='table') }}

SELECT
    marriage_id,
    person_id AS husband_id,
    person_id AS wife_id,
    name AS husband_name,
    name AS wife_name,
    child.family_id,
    marriage_precision,
    marriage_place
FROM {{ ref('int_people') }} people
LEFT JOIN {{ ref('stg_family_children') }} child
    ON people.id = child.child_id