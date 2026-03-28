import pandas as pd
from fredapi import Fred
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

DB_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

FRED_SERIES = {
    'FEDFUNDS': 'Federal Funds Rate',
    'CPIAUCSL': 'Consumer Price Index',
    'UNRATE':   'Unemployment Rate',
    'GS10':     'Ten Year Treasury Yield',
    'GDPC1':    'Real GDP'
}

def create_table(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bronze_macro_indicators (
                id SERIAL PRIMARY KEY,
                series_id VARCHAR(20),
                series_name VARCHAR(100),
                date DATE,
                value FLOAT,
                ingested_at TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.commit()

def ingest_series(series_id, series_name, fred, engine):
    data = fred.get_series(series_id, observation_start='2019-01-01')
    df = data.reset_index()
    df.columns = ['date', 'value']
    df['series_id'] = series_id
    df['series_name'] = series_name
    df = df[['series_id', 'series_name', 'date', 'value']]
    df.dropna(inplace=True)
    df.to_sql('bronze_macro_indicators', engine, if_exists='append', index=False)
    print(f"Ingested {len(df)} rows for {series_id} - {series_name}")

def run():
    fred = Fred(api_key=os.getenv('FRED_API_KEY'))
    engine = create_engine(DB_URL)
    create_table(engine)
    for series_id, series_name in FRED_SERIES.items():
        ingest_series(series_id, series_name, fred, engine)

if __name__ == "__main__":
    run()
