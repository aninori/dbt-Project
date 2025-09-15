{{ config(
    materialized='table',
    schema='silver'
) }}

WITH fct AS (
    SELECT
        d.Date_id,
        s.Store_id,
        s.Dept_id,
        s.Store_size,
        dpt.Weekly_Sales AS Store_weekly_sales,
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
    JOIN {{ source('source_data', 'FACT') }} f
        ON f.STORE = s.Store_id
    JOIN {{ source('source_data', 'DEPARTMENT') }} dpt
        ON dpt.STORE = s.Store_id
        AND dpt.DEPT = s.Dept_id
        AND dpt.DATE = f.DATE
    JOIN {{ ref('walmart_date_dim') }} d
        ON f.DATE = d.Store_Date
)

SELECT * FROM fct

