
-- leaving this in in case misinterpreted snapshot q
{# with sales_2018_2020 as (
    select * from {{ ref("stg_iowa_liquor__sales_2018_2020") }}
),

sales_2019_2021 as (
    select * from {{ ref("stg_iowa_liquor__sales_2019_2021") }}
    {% if is_incremental() %}
        where update_timestamp >= (select max(update_timestamp) from {{ this }})
    {% endif %}
),

-- union and only select latest updated per unique key
unioned as (
    select * from (
        select * from sales_2018_2020
        union all
        select * from sales_2019_2021
    )
    qualify update_timestamp = max(update_timestamp) over (partition by invoice_item_number)
),

-- check for removed in revised periods
removal as (
    select invoice_item_number
    from sales_2018_2020
    where invoice_item_number not in (select distinct invoice_item_number from sales_2019_2021)
),

-- remove removed in revised periods
final as (
    select * from unioned
    where invoice_item_number not in (select invoice_item_number from removal)
)

select * from final #}