{{ config(
    materialized='incremental',
    unique_key='invoice_item_number',
    incremental_strategy='merge',
    post_hook="
        {% if is_incremental() %}
            delete from {{ this }} 
            where (is_overlap = true and is_removed_recent = true)
        {% endif %}
    "
) }}




with sales_2018_2020_raw as (
    select * from {{ ref("stg_iowa_liquor__sales_2018_2020") }}
),

sales_2019_2021 as (
    select * from {{ ref("stg_iowa_liquor__sales_2019_2021") }}
),

sales_2018_2020 as (
    select *,
        case 
            when date_day::date between (select min(date_day) from sales_2019_2021)::date 
            and (select max(date_day) from sales_2018_2020_raw)::date then true else false 
        end as is_overlap,
        case
            when (src_name = 'iowa_liquor_sales__2018_2020' and invoice_item_number not in 
            (select invoice_item_number from sales_2019_2021)) then true else false 
        end as is_removed_recent
    from sales_2018_2020_raw

),

new_data as (
    select
        *
    from sales_2019_2021
),

{% if is_incremental() %}
    old_data as (
        select * from {{ this }}
    ),

    {# old_data as (
        select *,
            case 
                when date_day::date between (select min(date_day) from sales_2019_2021)::date 
                and (select max(date_day) from sales_2018_2020)::date then true else false 
            end as is_overlap,
            case
                when (src_name = 'iowa_liquor_sales__2018_2020' and invoice_item_number not in 
                (select invoice_item_number from sales_2019_2021)) then true else false 
            end as is_removed_recent
        from {{ this }}
        where not (is_overlap = true and is_removed_recent = true)
    ), #}

    unioned as (

        select *, null as is_overlap, null as is_removed_recent
        from new_data
        where invoice_item_number not in (
                select invoice_item_number
                from old_data
            )
        or update_timestamp
            > (
                select max(update_timestamp)
                from old_data
                where invoice_item_number = new_data.invoice_item_number
            )
        union all
        -- select * exclude (is_overlap, is_removed_recent)
        select *
        from old_data
        where invoice_item_number not in (
                select invoice_item_number
                from new_data
            )
        -- and update_timestamp <= (select max(update_timestamp) from old_data)

    ),

    final as (select * from unioned)

    {# helpers as (
        select
            *,
            case 
                when date_day::date between (select min(date_day) from sales_2019_2021)::date 
                and (select max(date_day) from sales_2018_2020)::date then true else false 
            end as is_overlap,
            case
                when (src_name = 'iowa_liquor_sales__2018_2020' and invoice_item_number not in 
                (select invoice_item_number from sales_2019_2021)) then true else false 
            end as is_removed_recent
        from unioned
    ),

    _final as (
        select
            *,
            case 
                when is_overlap = true and is_removed_recent = true then true else false
            end as to_be_removed
        from helpers
    ),

    final as (select * from _final where to_be_removed != true) #}

{% else %}

final as (
    select
        *
    from sales_2018_2020
)

{% endif %}

select * from final