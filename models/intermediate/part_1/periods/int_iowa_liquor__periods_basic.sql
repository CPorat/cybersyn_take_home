/*--------------------------------------------------------------------------------------------------
This code aggregates transactional data into weekly, monthly, and quarterly periods.
There are 3 CTEs that aggregate the base data into weekly, monthly, and quarterly granularity,
starting with a date spine:

- period_week:
    Joins the date spine to the base data, aggregates to the week level

- period_month:
    Joins the date spine to the base data, aggregates to the month level

- period_quarter:
    Joins the date spine to the base data, aggregates to the quarter level

These are unioned together into a single CTE called unioned.
--------------------------------------------------------------------------------------------------*/

with base as (
    select * from {{ ref('int_iowa_liquor__cleaned_stores') }}
),

date_dim as (
    select * from {{ ref('date_dim') }}
),

/*--------------------------------------------------------------------------------------------------
use date spine to ensure no missed weeks/months/quarters of data
--------------------------------------------------------------------------------------------------*/
date_spine as (
    select distinct
        iso_week_start_date as date_period,
        'week' as period_type
    from date_dim
    union all
    select distinct
        month_start_date as date_period,
        'month' as period_type
    from date_dim
    union all
    select distinct
        quarter_start_date as date_period,
        'quarter' as period_type
    from date_dim
),

period_week as (
    select
        'week' as period_type,
        date_spine.date_period as date_period_spine,
        {{ aggregate__to_period('week', 'BASE.DATE_DAY') }} as date_period,
        date_dim.prior_year_iso_week_start_date as prior_year_date_period,
        avg(pack_size) as avg_pack_size,
        mode(pack_size) as mode_pack_size,
        -- sum(bottle_volume_ml) as total_bottle_volume_ml,
        sum(bottles_sold) as total_bottles_sold,
        sum(sale_dollars) as total_sale_dollars,
        sum(volume_sold_liters) as total_volume_sold_liters,
        sum(volume_sold_gallons) as total_volume_sold_gallons
    from date_spine
    left join base
        on date_trunc('week', base.date_day) = date_spine.date_period
    left join date_dim
        on date_trunc('week', base.date_day) = date_dim.iso_week_start_date
    where date_spine.period_type = 'week'
    group by 1, 2, 3, 4
),

period_month as (
    select
        'month' as period_type,
        date_spine.date_period as date_period_spine,
        {{ aggregate__to_period('month', 'BASE.DATE_DAY') }} as date_period,
        date_dim.prior_year_month_start_date as prior_year_date_period,
        avg(pack_size) as avg_pack_size,
        mode(pack_size) as mode_pack_size,
        -- sum(bottle_volume_ml) as total_bottle_volume_ml,
        sum(bottles_sold) as total_bottles_sold,
        sum(sale_dollars) as total_sale_dollars,
        sum(volume_sold_liters) as total_volume_sold_liters,
        sum(volume_sold_gallons) as total_volume_sold_gallons
    from date_spine
    left join base
        on date_trunc('month', base.date_day) = date_spine.date_period
    left join date_dim
        on date_trunc('month', base.date_day) = date_dim.month_start_date
    where date_spine.period_type = 'month'
    group by 1, 2, 3, 4
),

period_quarter as (
    select
        'quarter' as period_type,
        date_spine.date_period as date_period_spine,
        {{ aggregate__to_period('quarter', 'BASE.DATE_DAY') }} as date_period,
        date_spine.date_period - interval '1 year' as prior_year_date_period,
        avg(pack_size) as avg_pack_size,
        mode(pack_size) as mode_pack_size,
        -- sum(bottle_volume_ml) as total_bottle_volume_ml,
        sum(bottles_sold) as total_bottles_sold,
        sum(sale_dollars) as total_sale_dollars,
        sum(volume_sold_liters) as total_volume_sold_liters,
        sum(volume_sold_gallons) as total_volume_sold_gallons
    from date_spine
    left join base
        on date_trunc('quarter', base.date_day) = date_spine.date_period
    {# left join date_dim
        on date_trunc('quarter', base.date_day) = date_dim.quarter_start_date #}
    where date_spine.period_type = 'quarter'
    group by 1, 2, 3, 4
),

unioned as (
    select * from period_week
    union all
    select * from period_month
    union all
    select * from period_quarter
)

select * from unioned
where date_period_spine <= '2021-01-01'
