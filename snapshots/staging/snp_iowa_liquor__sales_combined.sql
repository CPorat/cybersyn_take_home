{% snapshot snp_iowa_liquor__sales_combined %}

{{
    config(
        unique_key='invoice_item_number',
        strategy='timestamp',
        updated_at='update_timestamp',
        invalidate_hard_deletes = True
    )
}}

with raw_source as (

    select *
    from {{ ref('stg_iowa_liquor__sales_combined') }}

)

select * from raw_source

{% endsnapshot %}