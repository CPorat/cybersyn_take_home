{{ config(materialized="view") }}

with src as (
    select * from {{ ref('int_iowa_liquor__periods_all__yoy') }}
)

select * from src
