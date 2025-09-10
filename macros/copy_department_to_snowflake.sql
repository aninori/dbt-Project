{% macro copy_department_to_snowflake(table_name) %}

    delete from {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.{{ table_name }};

    copy into {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.{{ table_name }}
    from (
        select
            $1 as STORE,
            $2 as DEPT,
            $3 as DATE,
            $4 as WEEKLY_SALES,
            $5 as ISHOLIDAY
        from @{{ var('stage_name') }}/department.csv
    )
    file_format = {{ var('file_format_csv') }}
    purge = {{ var('purge_status') }}
    force = true;

{% endmacro %}

