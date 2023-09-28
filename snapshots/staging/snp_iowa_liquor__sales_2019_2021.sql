{% snapshot snp_iowa_liquor__sales_2019_2021 %}

{{
    config(
        unique_key='invoice_item_number',
        strategy='timestamp',
        updated_at='update_timestamp',
    )
}}

with raw_source as (

    select *
    from {{ source('IOWA_LIQUOR', 'IOWA_LIQUOR_SALES_2019_2021') }}

),

final as (

    select
        "Invoice/Item Number"::varchar as invoice_item_number,
        try_to_date("Date"::varchar) as date_day,
        "Store Number"::number as store_number,
        "Store Name"::varchar as store_name,
        "Address"::varchar as address,
        "City"::varchar as city,
        "Zip Code"::number as zip_code,
        "Store Location"::varchar as store_location,
        "County Number"::number as county_number,
        "County"::varchar as county,
        "Category"::number as category,
        "Category Name"::varchar as category_name,
        "Vendor Number"::number as vendor_number,
        "Vendor Name"::varchar as vendor_name,
        "Item Number"::number as item_number,
        "Item Description"::varchar as item_description,
        "Pack"::number as pack_size,
        "Bottle Volume (ml)"::number as bottle_volume_ml,
        "State Bottle Cost"::float as state_bottle_cost,
        "State Bottle Retail"::float as state_bottle_retail,
        "Bottles Sold"::number as bottles_sold,
        "Sale (Dollars)"::float as sale_dollars,
        "Volume Sold (Liters)"::float as volume_sold_liters,
        "Volume Sold (Gallons)"::float as volume_sold_gallons,
        "Update Timestamp"::timestamp_ltz as update_timestamp

    from raw_source

)

select * from final

{% endsnapshot %}