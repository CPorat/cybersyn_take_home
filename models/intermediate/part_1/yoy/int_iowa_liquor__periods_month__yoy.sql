with base as (
    select * from {{ ref("int_iowa_liquor__periods_all") }}
),

periods_month as (
    select * from base where period_type = 'month'
),

month_yoy as (
    select *
    from (
        {{ calculate__yoy_change(
            table_name='periods_month',
            match_key='period_key',
            date_field='date_period',
            prior_year_date_field='prior_year_date_period',
            label_fields=[
                'period_type',
                'store_name',
                'category_name',
                'vendor_name',
                'item_description'
            ],
            value_fields=[
                'avg_pack_size',
                'mode_pack_size',
                'total_bottles_sold',
                'total_sale_dollars',
                'total_volume_sold_liters',
                'total_volume_sold_gallons'
            ]
            )
        }}
    )
),

final as (
    select * from month_yoy
)

select * from final
