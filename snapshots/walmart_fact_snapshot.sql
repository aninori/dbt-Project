{% snapshot walmart_fact_snapshot %}
{{
  config(
    target_database='WALMART_SOURCE_DB',
    target_schema='SNAPSHOTS',
    unique_key=['Store_id', 'Dept_id'],
    strategy='check',
    check_cols=[
      'Store_size', 'Store_weekly_sales', 'Fuel_price', 'Temperature',
      'Unemployment', 'CPI',
      'Markdown1', 'Markdown2', 'Markdown3', 'Markdown4', 'Markdown5'
    ]
  )
}}

SELECT * FROM {{ ref('walmart_fact_table_source') }}

{% endsnapshot %}