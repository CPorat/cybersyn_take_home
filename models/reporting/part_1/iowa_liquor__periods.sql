{{ config(materialized="view") }}

with src as (
    select * from {{ ref('int_iowa_liquor__periods_all') }}
)

select * from src
