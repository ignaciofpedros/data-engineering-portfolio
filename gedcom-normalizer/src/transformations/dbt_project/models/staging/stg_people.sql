{{ config(materialized='view') }}

select
    id,
    name,
    birth as birth_raw

from read_csv_auto('../../../data/staging/people.csv')