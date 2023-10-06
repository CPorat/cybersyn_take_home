{{ config(materialized="view") }}

with src as (
    select * from {{ ref('int_iowa_liquor__incremental') }}
)

select * from src
