import pandas_datareader as pdr
import pandas as pd
from sqlalchemy import create_engine, text
import time
import os

try:
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable
    _conn = BaseHook.get_connection("marketpulse_postgres")
    DB_URL = f"postgresql+psycopg2://{_conn.login}:{_conn.password}@{_conn.host}:{_conn.port}/{_conn.schema}"
except Exception:
    from dotenv import load_dotenv
    load_dotenv()
    DB_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

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

def get_last_date(ticker, engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT MAX(date) FROM bronze_stock_prices WHERE ticker = :ticker
        """), {"ticker": ticker})
        return result.scalar()

def ingest_ticker(ticker, engine):
    last_date = get_last_date(ticker, engine)
    if last_date:
        start = str(last_date)
        print(f"{ticker}: last date in DB is {last_date}, fetching from {start}")
    else:
        start = "2019-01-01"
        print(f"{ticker}: no existing data, fetching from {start}")
    try:
        df = pdr.get_data_stooq(ticker, start=start)
        df = df.reset_index()
        df.columns = [c.lower() for c in df.columns]
        df['ticker'] = ticker
        df = df[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']]
        df.dropna(inplace=True)
        df = df[df['date'] > pd.Timestamp(last_date)] if last_date else df
        if len(df) == 0:
            print(f"{ticker}: no new rows to ingest")
        else:
            df.to_sql('bronze_stock_prices', engine, if_exists='append', index=False)
            print(f"Ingested {len(df)} new rows for {ticker}")
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