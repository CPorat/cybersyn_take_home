with iowa_liquor__cleaned_stores as (
    select * from {{ ref('int_iowa_liquor__cleaned_stores') }}
),

period_quarter as (
    {{ aggregate__iowa_liquor_to_period(
        table_name='iowa_liquor__cleaned_stores',
        period='quarter',
        timestamp_field='date_day'
    ) }}
),

final as (
    select * from period_quarter
)

select * from final
