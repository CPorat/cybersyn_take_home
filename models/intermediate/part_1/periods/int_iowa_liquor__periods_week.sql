with iowa_liquor__cleaned_stores as (
    select * from {{ ref('int_iowa_liquor__cleaned_stores') }}
),

period_week as (
    {{ aggregate__iowa_liquor_to_period(
        table_name='iowa_liquor__cleaned_stores',
        period='week',
        timestamp_field='date_day'
    ) }}
),

final as (
    select * from period_week
)

select * from final
where date_period <= '2021-01-01'
