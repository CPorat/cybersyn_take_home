with snapshotted as (
    select * from {{ ref("snp_iowa_liquor__sales_2018_2020") }}
)

select * from snapshotted where dbt_valid_to is null