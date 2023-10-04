with sales_2018 as (select * from {{ ref('stg_iowa_liquor__sales_2018_2020') }}),

sales_2019 as (select * from {{ ref('stg_iowa_liquor__sales_2019_2021') }}),

final as (
    select * from sales_2018
    union all
    select * from sales_2019
)

select * from final