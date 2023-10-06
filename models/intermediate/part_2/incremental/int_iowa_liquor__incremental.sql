{{ config(
    materialized='incremental',
    unique_key='invoice_item_number',
    incremental_strategy='merge'
) }}

{# with sales_2018 as (select * from {{ ref('stg_iowa_liquor__sales_2018_2020') }}),

sales_2019 as (select * from {{ ref('stg_iowa_liquor__sales_2019_2021') }}),

unioned as (
    select * from sales_2018
    union all
    select * from sales_2019
),

helpers as (
    select
        *,
        case 
            when date_day::date between (select min(date_day) from sales_2019)::date 
            and (select max(date_day) from sales_2018)::date then true else false 
        end as is_overlap,
        case
            when (src_name = 'iowa_liquor_sales__2018_2020' and invoice_item_number not in 
            (select invoice_item_number from sales_2019)) then true else false 
        end as is_removed_recent
    from unioned
),

final as (
    select * from helpers
    -- where is_overlap = true and is_removed_recent = false
)

select * from final
{% if is_incremental() %}
where update_timestamp > (
    select max(update_timestamp) from {{ this }}
)
{% endif %} #}

with sales_2018 as (select * from {{ ref('stg_iowa_liquor__sales_2018_2020') }}),

sales_2019 as (select * from {{ ref('stg_iowa_liquor__sales_2019_2021') }}),

unioned as (
    select * from sales_2018
    union all
    select * from sales_2019
),

{% if is_incremental() %}
    old_data as (
        select * from {{ this }}
    ),

    final as (
        select * from unioned
        where update_timestamp > (
            select max(update_timestamp) from old_data
        )
    )

{% else %}

    final as (
        select * from sales_2018
    )

{% endif %}

select * from final