{{ config(materialized='view') }}

SELECT
    ticker,
    date::date AS date,
    ROUND(open::numeric, 4) AS open,
    ROUND(high::numeric, 4) AS high,
    ROUND(low::numeric, 4) AS low,
    ROUND(close::numeric, 4) AS close,
    volume::bigint AS volume,
    ingested_at
FROM {{ source('bronze', 'bronze_stock_prices') }}
WHERE
    close IS NOT NULL
    AND volume > 0
    AND date IS NOT NULL