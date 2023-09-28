
{% macro aggregate__iowa_liquor_to_period(table_name, period, timestamp_field) %}

    /*--------------------------------------------------------------------------------------------------
    set variables and handle errors in period inputs
    --------------------------------------------------------------------------------------------------*/
    {% if period == 'week' %}
        {% set period_colname = 'iso_week_start_date' %}
        {% set period_prior_year_colname = 'prior_year_iso_week_start_date' %}
    {% elif period == 'month' %}
        {% set period_colname = 'month_start_date' %}
        {% set period_prior_year_colname = 'prior_year_month_start_date' %}
    {% elif period == 'quarter' %}
        {% set period_colname = 'quarter_start_date' %}
        {% set period_prior_year_colname = 'prior_year_quarter_start_date' %}
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid period: '" ~ period ~ "'. Period must be one of ['week', 'month', 'quarter'.") }}
    {% endif %}


    with _base_tbl as (
        select * from {{ table_name }}
    ),

    _date_dim as (
        select * from {{ ref('date_dim') }}
    ),

    /*--------------------------------------------------------------------------------------------------
    use date spine to ensure no missed weeks/months/quarters of data
    --------------------------------------------------------------------------------------------------*/
    _date_spine as (
        {# select distinct iso_week_start_date as date_period, 'week' as period_type from _date_dim
        union all
        select distinct month_start_date as date_period, 'month' as period_type from _date_dim
        union all
        select distinct quarter_start_date as date_period, 'quarter' as period_type from _date_dim
        where period_type = '{{ period }}' #}
        {% if period == 'week' %}
            select distinct iso_week_start_date as date_period, 'week' as period_type from _date_dim
        {% elif period == 'month' %}
            select distinct month_start_date as date_period, 'month' as period_type from _date_dim
        {% elif period == 'quarter' %}
            select distinct quarter_start_date as date_period, 'quarter' as period_type from _date_dim
        {% else %}
            {{ exceptions.raise_compiler_error("Invalid period: '" ~ period ~ "'. Period must be one of ['week', 'month', 'quarter'.") }}
        {% endif %}
    ),

    _period_grouped as (
        select
            _date_spine.period_type as period_type,
            _date_spine.date_period as date_period_spine,

            -- date trunc to get the week/month/quarter start date
            {% if period == 'week' %}
                date_trunc('week', _base_tbl.{{ timestamp_field }})
            {% elif period == 'month' %}
                date_trunc('month', _base_tbl.{{ timestamp_field }})
            {% elif period == 'quarter' %}
                date_trunc('quarter', _base_tbl.{{ timestamp_field }})
            {% else %}
                {{ exceptions.raise_compiler_error("Invalid period: '" ~ period ~ "'. Period must be one of ['week', 'month', 'quarter'.") }}
            {% endif %} as date_period,

            _date_dim.{{ period_prior_year_colname }} as prior_year_date_period,
            store_name,
            category_name,
            vendor_name,
            item_description,

            -- metrics
            avg(pack_size) as avg_pack_size,
            mode(pack_size) as mode_pack_size,
            -- sum(bottle_volume_ml) as total_bottle_volume_ml,
            sum(bottles_sold) as total_bottles_sold,
            sum(sale_dollars) as total_sale_dollars,
            sum(volume_sold_liters) as total_volume_sold_liters,
            sum(volume_sold_gallons) as total_volume_sold_gallons

        from _date_spine
        left join _base_tbl
            on date_trunc('{{ period }}', _base_tbl.date_day) = _date_spine.date_period
        left join _date_dim
            on date_trunc('{{ period }}', _base_tbl.date_day) = _date_dim.{{ period_colname }}
        group by 1,2,3,4,5,6,7,8
    ),

    _final as (
        select
            date_period_spine as date_period,
            * exclude (date_period, date_period_spine),

            -- generate surrogate key to join back on for previous year data
            {{ dbt_utils.generate_surrogate_key(
                [
                    'period_type',
                    'store_name',
                    'category_name',
                    'vendor_name',
                    'item_description'
                ]
                )
            }} as period_key

        from _period_grouped
    )

    select * from _final

{% endmacro %}
