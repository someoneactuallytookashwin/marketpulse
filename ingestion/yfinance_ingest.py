import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

DB_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

TICKERS = ['^GSPC', 'AAPL', 'JPM', 'GS', 'SPY']

def create_table(engine):
    with engine.connect() as conn:
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
        conn.commit()

def ingest_ticker(ticker, engine, start="2019-01-01"):
    df = yf.download(ticker, start=start, auto_adjust=True)
    df = df.reset_index()
    df.columns = [c[0].lower() if isinstance(c, tuple) else c.lower() for c in df.columns]
    df['ticker'] = ticker
    df = df[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']]
    df.to_sql('bronze_stock_prices', engine, if_exists='append', index=False)
    print(f"Ingested {len(df)} rows for {ticker}")

def run():
    engine = create_engine(DB_URL)
    create_table(engine)
    for ticker in TICKERS:
        ingest_ticker(ticker, engine)

if __name__ == "__main__":
    run()