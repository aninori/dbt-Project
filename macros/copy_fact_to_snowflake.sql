{% macro copy_fact_to_snowflake(table_name) %}

    delete from {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.{{ table_name }};

    copy into {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.{{ table_name }}
    from (
        select
            $1 as STORE,
            $2 as DATE,
            $3 as TEMPERATURE,
            $4 as FUEL_PRICE,
            $5 as MARKDOWN1,
            $6 as MARKDOWN2,
            $7 as MARKDOWN3,
            $8 as MARKDOWN4,
            $9 as MARKDOWN5,
            $10 as CPI,
            $11 as UNEMPLOYMENT,
            $12 as ISHOLIDAY
        from @{{ var('stage_name') }}/fact.csv
    )
    file_format = {{ var('file_format_csv') }}
    purge = {{ var('purge_status') }}
    force = true;

{% endmacro %}
