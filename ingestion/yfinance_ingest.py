import yfinance as yf
import pandas_datareader as pdr
import pandas as pd
from sqlalchemy import create_engine, text
import time
import os

try:
    from airflow.hooks.base import BaseHook
    _conn = BaseHook.get_connection("marketpulse_postgres")
    DB_URL = f"postgresql+psycopg2://{_conn.login}:{_conn.password}@{_conn.host}:{_conn.port}/{_conn.schema}"
    USE_STOOQ = True
except Exception:
    from dotenv import load_dotenv
    load_dotenv()
    DB_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    USE_STOOQ = False

TICKERS = ['SPX', 'AAPL', 'JPM', 'GS', 'SPY']
TICKER_MAP = {
    'SPX': '^SPX'
}

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
    fetch_ticker = TICKER_MAP.get(ticker, ticker)
    last_date = get_last_date(ticker, engine)
    start = "2020-01-01" if not last_date else str(last_date)
    print(f"{ticker}: fetching from {start}")
    try:
        if USE_STOOQ:
            df = pdr.get_data_stooq(fetch_ticker, start=start)
            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]
        else:
            df = yf.download(fetch_ticker, start=start, auto_adjust=True, progress=False)
            df = df.reset_index()
            df.columns = [c[0].lower() if isinstance(c, tuple) else c.lower() for c in df.columns]
        if df.empty:
            print(f"{ticker}: no data returned")
            return
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