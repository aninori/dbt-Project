{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='Date'  -- based on natural key,
    pre_hook=copy_department_to_snowflake('department')
) }}

WITH source AS (
    SELECT DISTINCT
        Date AS Store_Date,
        IsHoliday,
        CURRENT_TIMESTAMP() AS Insert_date,
        CURRENT_TIMESTAMP() AS Update_date
    FROM {{ this }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY Store_Date) AS Date_Id,
    Store_Date,
    IsHoliday,
    Insert_date,
    Update_date
FROM source

{% if is_incremental() %}
-- dbt will auto-handle delete+insert on unique_key = Date
{% endif %}
