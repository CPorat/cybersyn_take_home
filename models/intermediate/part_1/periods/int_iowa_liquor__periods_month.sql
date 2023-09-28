with iowa_liquor__cleaned_stores as (
    select * from {{ ref('int_iowa_liquor__cleaned_stores') }}
),

period_month as (
    {{ aggregate__iowa_liquor_to_period(
        table_name='iowa_liquor__cleaned_stores',
        period='month',
        timestamp_field='date_day'
    ) }}
),

final as (
    select * from period_month
)

select * from final
