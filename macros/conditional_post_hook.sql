{% macro conditional_post_hook_macro() %}

{{ log("------ is_incremental = " ~ is_incremental()) }}
{% if is_incremental() %}
    {{ log("------ Incremental Load -------")  }}
    {{ return("select 'Incremental Load Post Hook' as txt") }}
{% else %}
    {{ log("------ Initial Load -------")  }}
    {{ return("select 'Initial Load Post Hook' as txt") }}
{% endif %}

{% endmacro %}