with src as (select * from {{ ref('snp_iowa_liquor__sales_combined') }}),

-- get version number and far future date
final as (
    select
        * exclude (dbt_valid_to),
        coalesce(dbt_valid_to, '{{ var('the_distant_future') }}'::timestamp_ltz) as dbt_valid_to,
        row_number() over (partition by invoice_item_number order by dbt_valid_from) as version
    from src
)

select * from final