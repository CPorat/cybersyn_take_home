with base as (
    select *
    from {{ ref('stg_iowa_liquor__sales_2018_2020') }}
),

cleaned as (
    select
        upper(store_name) as store_name_raw,
        trim(regexp_replace(split_part(store_name_raw, '/', 1), '''', ''))
            as store_name_split,
        regexp_replace(
            regexp_replace(store_name_split, '#[0-9]+$', ''), '[.,]', ''
        ) as store_name_preprocess,
        case
            when store_name_preprocess ilike '%wal-mart%' then 'WAL-MART'
            when store_name_preprocess ilike '%target%' then 'TARGET'
            when store_name_preprocess ilike '%hy-vee%' then 'HY-VEE'
            when store_name_preprocess ilike '%sams club%' then 'SAMS CLUB'
            when store_name_preprocess ilike '%walgreens%' then 'WALGREENS'
            when store_name_preprocess ilike '%cvs%' then 'CVS'
            when store_name_preprocess ilike '%kum & go%' then 'KUM & GO'
            when store_name_preprocess ilike '%fareway%' then 'FAREWAY'
            when store_name_preprocess ilike '%smokin joes%' then 'SMOKIN JOES'
            when store_name_preprocess ilike '%quik trip%' then 'QUIK TRIP'
            when store_name_preprocess ilike '%yesway%' then 'YESWAY'
            when
                store_name_preprocess ilike '%caseys general store%'
                then 'CASEYS GENERAL STORE'
            else store_name_preprocess
        end as store_name,
        * exclude (store_name)
    from base
)

select * exclude (store_name_raw, store_name_split, store_name_preprocess)
from cleaned
