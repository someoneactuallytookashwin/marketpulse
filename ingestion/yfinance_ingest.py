import pandas_datareader as pdr
import pandas as pd
from sqlalchemy import create_engine, text
import time

DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

TICKERS = ['^SPX', 'AAPL', 'JPM', 'GS', 'SPY']

def create_table(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bronze_stock_prices (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(20),
                date DATE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                ingested_at TIMESTAMP DEFAULT NOW()
            )
        """))

def ingest_ticker(ticker, engine, start="2019-01-01"):
    try:
        df = pdr.get_data_stooq(ticker, start=start)
        df = df.reset_index()
        df.columns = [c.lower() for c in df.columns]
        df['ticker'] = ticker
        df = df[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']]
        df.dropna(inplace=True)
        df.to_sql('bronze_stock_prices', engine, if_exists='append', index=False)
        print(f"Ingested {len(df)} rows for {ticker}")
    except Exception as e:
        print(f"Failed {ticker}: {e}")
    time.sleep(2)

def run():
    engine = create_engine(DB_URL)
    create_table(engine)
    for ticker in TICKERS:
        ingest_ticker(ticker, engine)

if __name__ == "__main__":
    run()