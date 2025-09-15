{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='Store_Date',
    pre_hook=copy_department_to_snowflake('department')
) }}

WITH source AS (
    SELECT DISTINCT
        Date AS Store_Date,
        IsHoliday,
        CURRENT_TIMESTAMP() AS Insert_date,
        CURRENT_TIMESTAMP() AS Update_date
    FROM {{ source('source_data', 'DEPARTMENT') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY Store_Date) AS Date_Id,
    Store_Date,
    IsHoliday,
    Insert_date,
    Update_date
FROM source

{% if is_incremental() %}
-- dbt handles the delete+insert using unique_key = 'Store_Date'
{% endif %}



