{{ config(materialized="view") }}

with period_week as (
    select * from {{ ref('int_iowa_liquor__periods_week') }}
),

period_month as (
    select * from {{ ref('int_iowa_liquor__periods_month') }}
),

period_quarter as (
    select * from {{ ref('int_iowa_liquor__periods_quarter') }}
),

final as (
    select * from period_week
    union all
    select * from period_month
    union all
    select * from period_quarter
)

select * from final
where date_period <= '2021-01-01'
