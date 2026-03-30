{{ config(materialized='table') }}

WITH macro AS (
    SELECT
        date,
        MAX(CASE WHEN series_id = 'FEDFUNDS' THEN value END) AS fed_funds_rate,
        MAX(CASE WHEN series_id = 'CPIAUCSL' THEN value END) AS cpi,
        MAX(CASE WHEN series_id = 'UNRATE' THEN value END) AS unemployment_rate,
        MAX(CASE WHEN series_id = 'GS10' THEN value END) AS treasury_10y_yield
    FROM {{ ref('silver_macro_indicators') }}
    GROUP BY date
),

spy AS (
    SELECT
        date,
        close AS spy_close,
        rolling_30d_volatility
    FROM {{ ref('gold_stock_performance') }}
    WHERE ticker = 'SPY'
)

SELECT
    m.date,
    m.fed_funds_rate,
    m.cpi,
    m.unemployment_rate,
    m.treasury_10y_yield,
    s.spy_close,
    s.rolling_30d_volatility
FROM macro m
LEFT JOIN spy s ON s.date = m.date
ORDER BY m.date