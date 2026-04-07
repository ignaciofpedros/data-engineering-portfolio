{{ config(materialized='view') }}

SELECT
    f.family_id,
    f.husband_id,
    h.name AS husband_name,
    f.wife_id,
    w.name AS wife_name,
    f.marriage_date_id,
    f.date_precision,
    p.place_name AS marriage_place_name
FROM {{ ref('int_family') }} f
LEFT JOIN {{ ref('int_people') }} h ON f.husband_id = h.id
LEFT JOIN {{ ref('int_people') }} w ON f.wife_id = w.id
LEFT JOIN {{ ref('dim_place') }} p ON f.marriage_place_id = p.place_id
