with sales_2018_2020 as (
    select
        *,
        'iowa_liquor__sales_2018_2020' as dataset_name
    from {{ ref("stg_iowa_liquor__sales_2018_2020") }}
),

sales_2019_2021 as (
    select
        *,
        'iowa_liquor__sales_2019_2021' as dataset_name
    from {{ ref("stg_iowa_liquor__sales_2019_2021") }}
),

combined as (
    select * from (
        select * from sales_2018_2020
        union all
        select * from sales_2019_2021
    )
    qualify update_timestamp = max(update_timestamp) over (partition by invoice_item_number)
)

select * from combined