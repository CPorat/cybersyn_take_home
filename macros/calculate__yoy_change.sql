{% macro calculate__yoy_change(table_name, match_key, date_field, prior_year_date_field, label_fields, value_fields) %}

/*--------------------------------------------------------------------------------------------------
This macro calculates the year-over-year percentage change for a set of value fields.

It joins the base table to itself, lining up the current and prior year records based on the match key.

Then it calculates the yoy change as:

(current_value - prior_year_value) / prior_year_value

Arguments:

- table_name: The name of the base table to calculate over
- match_key: The field to match current and prior year records on
- date_field: The date field to use for the current year
- prior_year_date_field: The date field to use for the prior year
- label_fields: The fields to carry over verbatim from the base table
- value_fields: The numeric fields to calculate yoy change for

Returns:

A table with the label fields, current/prior year value fields, and yoy change fields.
*/--------------------------------------------------------------------------------------------------

with _base_calc as (

    select
        {{ date_field }},
        {{ prior_year_date_field }},
        {{ match_key }},

        -- for loop over label fields
        {%- for label_field in label_fields %}
        {{ label_field }},
        {%- endfor %}

        -- for loop over value fields
        {%- for value_field in value_fields %}
        {{ value_field }}{% if not loop.last %},{% endif %}
        {%- endfor %}

    from {{ table_name }}
),

_joined as (

    select
        _base_calc.{{ date_field }},
        _base_calc.{{ prior_year_date_field }},

        -- for loop over label fields
        {%- for label_field in label_fields %}
        _base_calc.{{ label_field }},
        {%- endfor %}

        -- for loop over value fields to get current, prior year values
        -- and calculate yoy change
        {%- for value_field in value_fields %}
        _base_calc.{{ value_field }} as {{ value_field }}_current_year,
        _base_calc_prior_year.{{ value_field }} as {{ value_field }}_prior_year,
        _base_calc.{{ value_field }}
            / nullif(_base_calc_prior_year.{{ value_field }}, 0) - 1
            as {{ value_field }}_yoy_change{% if not loop.last %},{% endif %}
        {%- endfor %}

    from _base_calc
    left join _base_calc _base_calc_prior_year
        on _base_calc_prior_year.{{ date_field }} = _base_calc.{{ prior_year_date_field }}
        and _base_calc_prior_year.{{ match_key }} = _base_calc.{{ match_key }}
)

select * from _joined

{% endmacro %}