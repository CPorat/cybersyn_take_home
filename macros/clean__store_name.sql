{% macro clean__store_name(column_name) %}

    -- Split the column on the first '/' and trim any leading or trailing spaces
    trim(regexp_replace(split_part({{ column_name }}, '/', 1), '''', '')) as {{ column_name }}_split,

    -- Remove any trailing '#[0-9]+' or any '.' or ',' from the split column
    regexp_replace(
        regexp_replace({{ column_name }}_split, '#[0-9]+$', ''), '[.,]', ''
    ) as {{ column_name }}_preprocess,

    -- Match the preprocessed column to a set of predefined store names and replace them with a standard name
    -- If no match is found, keep the preprocessed name
    case
        when {{ column_name }}_preprocess ilike '%wal-mart%' then 'WAL-MART'
        when {{ column_name }}_preprocess ilike '%target%' then 'TARGET'
        when {{ column_name }}_preprocess ilike '%hy-vee%' then 'HY-VEE'
        when {{ column_name }}_preprocess ilike '%sams club%' then 'SAMS CLUB'
        when {{ column_name }}_preprocess ilike '%walgreens%' then 'WALGREENS'
        when {{ column_name }}_preprocess ilike '%cvs%' then 'CVS'
        when {{ column_name }}_preprocess ilike '%kum & go%' then 'KUM & GO'
        when {{ column_name }}_preprocess ilike '%fareway%' then 'FAREWAY'
        when {{ column_name }}_preprocess ilike '%smokin joes%' then 'SMOKIN JOES'
        when {{ column_name }}_preprocess ilike '%quik trip%' then 'QUIK TRIP'
        when {{ column_name }}_preprocess ilike '%yesway%' then 'YESWAY'
        when {{ column_name }}_preprocess ilike '%caseys general store%' then 'CASEYS GENERAL STORE'
        else {{ column_name }}_preprocess
    end as {{ column_name }}_clean

{% endmacro %}