{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['Store_id', 'Dept_id'],
    pre_hook=["{{ copy_department_to_snowflake('DEPARTMENT') }}", "{{ copy_stores_to_snowflake('STORES') }}"]
) }}

WITH source AS (
    SELECT DISTINCT
        dept.STORE AS Store_id,
        dept.DEPT AS Dept_id,
        st.TYPE AS Store_type,
        st.SIZE AS Store_size,
        CURRENT_TIMESTAMP() AS Insert_date,
        CURRENT_TIMESTAMP() AS Update_date
    FROM {{ source('source_data', 'DEPARTMENT') }} dept
    LEFT JOIN {{ source('source_data', 'STORES') }} st
        ON dept.STORE = st.STORE
)

SELECT * FROM source
