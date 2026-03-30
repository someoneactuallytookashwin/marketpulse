{{ config(materialized='view') }}

SELECT
    series_id,
    series_name,
    date::date AS date,
    ROUND(value::numeric, 4) AS value,
    ingested_at
FROM {{ source('bronze', 'bronze_macro_indicators') }}
WHERE
    value IS NOT NULL
    AND date IS NOT NULL