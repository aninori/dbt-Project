{{ config(
    materialized='table',
    schema='silver',
    pre_hook=["{{ copy_department_to_snowflake('DEPARTMENT') }}", "{{ copy_stores_to_snowflake('STORES') }}","{{copy_fact_to_snowflake('FACT')}}"]
) }}

WITH base AS (
    SELECT
        s.Store_id,
        s.Dept_id,
        s.Store_size,
        dpt.WEEKLY_SALES AS Store_weekly_sales,
        f.Fuel_price,
        f.Temperature,
        f.Unemployment,
        f.CPI,
        f.Markdown1,
        f.Markdown2,
        f.Markdown3,
        f.Markdown4,
        f.Markdown5,
        CURRENT_TIMESTAMP() AS insert_date,
        CURRENT_TIMESTAMP() AS update_date
    FROM {{ ref('walmart_store_dim') }} s
    LEFT JOIN {{ source('source_data', 'FACT') }} f
        ON s.Store_id = f.STORE
    LEFT JOIN {{ source('source_data', 'DEPARTMENT') }} dpt
        ON s.Store_id = dpt.STORE
        AND s.Dept_id = dpt.DEPT
        AND f.DATE = dpt.DATE
)

SELECT * FROM base


