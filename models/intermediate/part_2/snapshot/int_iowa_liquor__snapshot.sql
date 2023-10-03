with sales_2018_2020 as (
    select * from {{ ref("snp_iowa_liquor__sales_2018_2020") }}
),

sales_2019_2021 as (
    select * from {{ ref("snp_iowa_liquor__sales_2019_2021") }}
),

unioned as (
    select
        *
    from (
        select * from sales_2018_2020
        union all
        select * from sales_2019_2021
    )
    -- dedupe on latest timestamp
    -- qualify update_timestamp = max(update_timestamp) over (partition by invoice_item_number)
),

-- get version number and far future date
final as (
    select
        * exclude (dbt_valid_to),
        coalesce(dbt_valid_to, '{{ var('the_distant_future') }}'::timestamp_ltz) as dbt_valid_to,
        row_number() over (partition by invoice_item_number order by dbt_valid_from) as version
    from unioned
)

select * from final