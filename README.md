# MarketPulse

A daily financial data pipeline that ingests stock prices and macroeconomic indicators, transforms them through a medallion architecture using dbt, and surfaces insights via a Metabase dashboard.

## Stack

| Layer          | Technology                         |
| -------------- | ---------------------------------- |
| Orchestration  | Apache Airflow 2.8 (LocalExecutor) |
| Ingestion      | Python — yfinance, fredapi         |
| Warehouse      | PostgreSQL 15                      |
| Transformation | dbt (bronze / silver / gold)       |
| BI             | Metabase                           |
| Infrastructure | Docker, Docker Compose             |

## Architecture

```
Yahoo Finance  -->  yfinance_ingest.py  -->  bronze_stock_prices
FRED API       -->  fred_ingest.py      -->  bronze_macro_indicators
                                                      |
                                             dbt silver (views)
                                        silver_stock_prices
                                        silver_macro_indicators
                                                      |
                                              dbt gold (tables)
                                        gold_stock_performance
                                        gold_market_macro
                                                      |
                                               Metabase
```

**Bronze** — raw ingestion, append-only  
**Silver** — cleaned views: null filtering, type casting, validation  
**Gold** — aggregated tables: daily returns, 30-day rolling volatility, macro overlays

## Prerequisites

- Docker and Docker Compose
- A FRED API key (free at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html))

## Running Locally

1. Clone the repository:

```bash
git clone https://github.com/someoneactuallytookashwin/marketpulse.git
cd marketpulse
```

2. Create a `.env` file in the project root with your database credentials, Airflow config, and Metabase config. The compose file reads from this file at startup.

3. Start all services:

```bash
docker-compose up --build -d
```

4. Once the containers are healthy, access the Airflow and Metabase UIs on the ports defined in your `.env` file.

**Postgres connection**

In the Airflow UI go to Admin → Connections and create a new connection:

| Field           | Value                  |
| --------------- | ---------------------- |
| Connection ID   | `marketpulse_postgres` |
| Connection Type | Postgres               |
| Host            | `postgres`             |
| Schema          | your database name     |
| Login           | your database user     |
| Password        | your database password |
| Port            | `5432`                 |

**FRED API key**

In the Airflow UI go to Admin → Variables and create a new variable:

| Key            | Value             |
| -------------- | ----------------- |
| `FRED_API_KEY` | your FRED API key |

The ingestion scripts will pick up the Airflow connection and variable when running inside Airflow. When running locally outside of Airflow, they fall back to reading from your `.env` file.

## Running dbt

Run the dbt models after the first ingestion completes:

```bash
cd dbt_project/marketpulse
dbt run
dbt test
```

## Changing Tracked Tickers

The pipeline currently tracks: `SPY`, `AAPL`, `JPM`, `GS`, `SPX`

To add or swap tickers, edit the `TICKERS` list in [ingestion/yfinance_ingest.py](ingestion/yfinance_ingest.py):

```python
TICKERS = ['SPY', 'AAPL', 'JPM', 'GS', 'SPX']
```

Tickers must match Yahoo Finance symbols. You can search for valid symbols at [finance.yahoo.com](https://finance.yahoo.com). If a ticker uses a different symbol on Yahoo Finance versus how you want to store it internally, add a mapping to `TICKER_MAP`:

```python
TICKER_MAP = {
    'SPX': '^GSPC'  # internal name -> Yahoo Finance symbol
}
```

After adding new tickers, update the `accepted_values` test in [dbt_project/marketpulse/models/silver/schema.yml](dbt_project/marketpulse/models/silver/schema.yml) to keep the data quality tests in sync.

## Changing Macro Indicators

The pipeline pulls FEDFUNDS, CPIAUCSL, UNRATE, GS10, and GDPC1 from the FRED API. To add or change series, edit the `FRED_SERIES` dictionary in [ingestion/fred_ingest.py](ingestion/fred_ingest.py). Valid series IDs can be browsed at [fred.stlouisfed.org](https://fred.stlouisfed.org).

## Project Structure

```
marketpulse/
├── dags/
│   └── marketpulse_dag.py        # Airflow DAG — daily schedule
├── ingestion/
│   ├── yfinance_ingest.py        # Stock price ingestion
│   └── fred_ingest.py            # Macro indicator ingestion
├── dbt_project/
│   └── marketpulse/
│       └── models/
│           ├── silver/           # Cleaned views + data quality tests
│           └── gold/             # Aggregated tables for BI
├── docker-compose.yml
├── Dockerfile
└── init-db.sh                    # Creates the Metabase database on first run
```
