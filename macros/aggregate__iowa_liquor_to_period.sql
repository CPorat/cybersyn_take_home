
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
        select distinct
            {{ period_colname }},
            {{ period_prior_year_colname }}
        from {{ ref('date_dim') }}
    ),

    /*--------------------------------------------------------------------------------------------------
    use date spine to ensure no missed weeks/months/quarters of data
    --------------------------------------------------------------------------------------------------*/
    _date_spine_base as (
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

    _date_spine as (
        select distinct
            _date_spine_base.date_period,
            _date_spine_base.period_type,
            _base_tbl.store_name,
            _base_tbl.category_name,
            _base_tbl.vendor_name,
            _base_tbl.item_description
        from _date_spine_base
        cross join _base_tbl
    ),

    _period_grouped as (
        select
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

        from _base_tbl
        group by 1,2,3,4,5
    ),

    _dates_add as (
        select
            _period_grouped.*,
            _date_dim.{{ period_prior_year_colname }} as prior_year_date_period
        from _period_grouped
        left join _date_dim
            on date_trunc('{{ period }}', _period_grouped.date_period) = _date_dim.{{ period_colname }}
    ),

    _joined_spine as (
        select
            _date_spine.*,
            _dates_add.prior_year_date_period,
            _dates_add.avg_pack_size,
            _dates_add.mode_pack_size,
            _dates_add.total_bottles_sold,
            _dates_add.total_sale_dollars,
            _dates_add.total_volume_sold_liters,
            _dates_add.total_volume_sold_gallons
        from _date_spine
        left join _dates_add
            on _date_spine.date_period = _dates_add.date_period
            and _date_spine.store_name = _dates_add.store_name
            and _date_spine.category_name = _dates_add.category_name
            and _date_spine.vendor_name = _dates_add.vendor_name
            and _date_spine.item_description = _dates_add.item_description
    ),

    _final as (
        select
            *,
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

        from _joined_spine
    )

    select * from _final

{% endmacro %}
