{{ config(materialized='table') }}

WITH daily_returns AS (
    SELECT
        ticker,
        date,
        close,
        LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS prev_close
    FROM {{ ref('silver_stock_prices') }}
),

returns AS (
    SELECT
        ticker,
        date,
        close,
        ROUND(((close - prev_close) / prev_close * 100)::numeric, 4) AS daily_return_pct
    FROM daily_returns
    WHERE prev_close IS NOT NULL
)

SELECT
    ticker,
    date,
    close,
    daily_return_pct,
    ROUND(AVG(daily_return_pct) OVER (
        PARTITION BY ticker ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )::numeric, 4) AS rolling_30d_avg_return,
    ROUND(STDDEV(daily_return_pct) OVER (
        PARTITION BY ticker ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )::numeric, 4) AS rolling_30d_volatility
FROM returns
ORDER BY ticker, date