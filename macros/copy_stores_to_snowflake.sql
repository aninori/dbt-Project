{% macro copy_stores_to_snowflake(table_name) %}

    delete from {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.{{ table_name }};

    copy into {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.{{ table_name }}
    from (
        select
            $1 as STORE,
            $2 as TYPE,
            $3 as SIZE
        from @{{ var('stage_name') }}/stores.csv
    )
    file_format = {{ var('file_format_csv') }}
    purge = {{ var('purge_status') }}
    force = true;

{% endmacro %}
