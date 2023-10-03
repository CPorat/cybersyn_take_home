{# with sales_2018_2020 as (
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
    -- qualify update_timestamp = max(update_timestamp) over (partition by invoice_item_number)
)

select * from combined #}

with raw_source_2018 as (

    select *
    from {{ source('IOWA_LIQUOR', 'IOWA_LIQUOR_SALES_2018_2020') }}

),

final_2018 as (

    select
        "Invoice/Item Number"::varchar as invoice_item_number,
        try_to_date("Date"::varchar) as date_day,
        "Store Number"::number as store_number,
        coalesce("Store Name"::varchar, 'NO STORE NAME') as store_name,
        "Address"::varchar as address,
        "City"::varchar as city,
        "Zip Code"::number as zip_code,
        "Store Location"::varchar as store_location,
        "County Number"::number as county_number,
        "County"::varchar as county,
        "Category"::number as category,
        coalesce("Category Name"::varchar, 'NO CATEGORY NAME') as category_name,
        "Vendor Number"::number as vendor_number,
        coalesce("Vendor Name"::varchar, 'NO VENDOR NAME') as vendor_name,
        "Item Number"::number as item_number,
        coalesce("Item Description"::varchar, 'NO ITEM DESCRIPTION') as item_description,
        "Pack"::number as pack_size,
        "Bottle Volume (ml)"::number as bottle_volume_ml,
        "State Bottle Cost"::float as state_bottle_cost,
        "State Bottle Retail"::float as state_bottle_retail,
        "Bottles Sold"::number as bottles_sold,
        "Sale (Dollars)"::float as sale_dollars,
        "Volume Sold (Liters)"::float as volume_sold_liters,
        "Volume Sold (Gallons)"::float as volume_sold_gallons,
        "Update Timestamp"::timestamp_ltz as update_timestamp

    from raw_source_2018

),

raw_source_2019 as (

    select *
    from {{ source('IOWA_LIQUOR', 'IOWA_LIQUOR_SALES_2019_2021') }}

),

final_2019 as (

    select
        "Invoice/Item Number"::varchar as invoice_item_number,
        try_to_date("Date"::varchar) as date_day,
        "Store Number"::number as store_number,
        coalesce("Store Name"::varchar, 'NO STORE NAME') as store_name,
        "Address"::varchar as address,
        "City"::varchar as city,
        "Zip Code"::number as zip_code,
        "Store Location"::varchar as store_location,
        "County Number"::number as county_number,
        "County"::varchar as county,
        "Category"::number as category,
        coalesce("Category Name"::varchar, 'NO CATEGORY NAME') as category_name,
        "Vendor Number"::number as vendor_number,
        coalesce("Vendor Name"::varchar, 'NO VENDOR NAME') as vendor_name,
        "Item Number"::number as item_number,
        coalesce("Item Description"::varchar, 'NO ITEM DESCRIPTION') as item_description,
        "Pack"::number as pack_size,
        "Bottle Volume (ml)"::number as bottle_volume_ml,
        "State Bottle Cost"::float as state_bottle_cost,
        "State Bottle Retail"::float as state_bottle_retail,
        "Bottles Sold"::number as bottles_sold,
        "Sale (Dollars)"::float as sale_dollars,
        "Volume Sold (Liters)"::float as volume_sold_liters,
        "Volume Sold (Gallons)"::float as volume_sold_gallons,
        "Update Timestamp"::timestamp_ltz as update_timestamp

    from raw_source_2019

),

final as (
    select * from final_2018
    union all
    select * from final_2019
)

select * from final