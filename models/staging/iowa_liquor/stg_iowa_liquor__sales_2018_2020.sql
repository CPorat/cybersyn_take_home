with raw_source as (

    select *
    from {{ source('IOWA_LIQUOR', 'IOWA_LIQUOR_SALES_2018_2020') }}

),

final as (

    select
        'iowa_liquor_sales__2018_2020' as src_name,
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
        "Update Timestamp"::timestamp_ltz as update_timestamp,
        min(update_timestamp) over () as min_update_timestamp,
        max(update_timestamp) over () as max_update_timestamp

    from raw_source

)

select * from final