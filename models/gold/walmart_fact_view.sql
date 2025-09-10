{{ config(
    materialized='view',
    schema='gold',
    alias='WALMART_FACT_VIEW'
) }}

WITH view AS (
  SELECT
    Store_id,
    Dept_id,
    Store_size,
    Store_weekly_sales,
    Fuel_price,
    Temperature,
    Unemployment,
    CPI,
    Markdown1,
    Markdown2,
    Markdown3,
    Markdown4,
    Markdown5,
    DBT_VALID_FROM AS vrsn_start_date,
    DBT_VALID_TO AS vrsn_end_date,
    insert_date,
    update_date
  FROM {{ ref('walmart_fact_snapshot') }}
)

SELECT * FROM view
