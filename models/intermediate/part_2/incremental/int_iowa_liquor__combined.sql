{{ config(materialized='incremental', unique_key='invoice_item_number') }}

with sales_2018_2020 as (
    select * from {{ ref("stg_iowa_liquor__sales_2018_2020") }}
),

sales_2019_2021 as (
    select * from {{ ref("stg_iowa_liquor__sales_2019_2021") }}
),

new_data as (
    select * from (
        select * from sales_2018_2020
        union all
        select * from sales_2019_2021
    )
    qualify update_timestamp = max(update_timestamp) over (partition by invoice_item_number)
),

{% if is_incremental() %}
    old_data as (
        select * from {{ this }}
    ),

    final as (

        select *
        from new_data
        where (invoice_item_number, update_timestamp) not in (
                select
                    invoice_item_number,
                    update_timestamp
                from old_data
            )
            or update_timestamp
            > (
                select max(update_timestamp)
                from old_data
                where invoice_item_number = new_data.invoice_item_number
            )
        union all
        select *
        from old_data
        where (invoice_item_number, update_timestamp) not in (
                select
                    invoice_item_number,
                    update_timestamp
                from new_data
            )
            and update_timestamp <= (select max(update_timestamp) from old_data)

    )

{% else %}

final as (
    select *
    from new_data
)

{% endif %}

select * from final
