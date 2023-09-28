with base as (
    {{ dbt_date.get_date_dimension("2017-12-01", "2100-12-31") }}
),

final as (
    select
        *,
        quarter_start_date - interval '1 year' as prior_year_quarter_start_date
    from base
    where date_day <= current_date()
)

select * from final
