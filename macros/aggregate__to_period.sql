{% macro aggregate__to_period(period, timestamp_field) %}

    {% if period == 'week' %}
        date_trunc('week', {{ timestamp_field }})
    {% elif period == 'month' %}
        date_trunc('month', {{ timestamp_field }})
    {% elif period == 'quarter' %}
        date_trunc('quarter', {{ timestamp_field }})
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid period: '" ~ period ~ "'. Period must be one of ['week', 'month', 'quarter'.") }}
    {% endif %}

{% endmacro %}
