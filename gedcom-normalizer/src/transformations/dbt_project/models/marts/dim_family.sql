{{ config(materialized='view') }}

SELECT
    f.family_id,
    f.husband_id,
    h.name AS husband_name,
    f.wife_id,
    w.name AS wife_name,
    f.id_marriage_date,
    f.date_precision,
    f.id_marriage_place
FROM {{ ref('int_family') }} f
LEFT JOIN {{ ref('int_people') }} h ON f.husband_id = h.id
LEFT JOIN {{ ref('int_people') }} w ON f.wife_id = w.id
