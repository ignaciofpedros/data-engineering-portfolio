{{ config(materialized='view') }}

SELECT
    id AS person_id,
    child.family_id,
    name,
    TRY_CAST(birth_date AS DATE) AS birth_date
FROM {{ ref('int_people') }} people
LEFT JOIN {{ ref('stg_family_children') }} child
    ON people.id = child.child_id