import pandas as pd
from fredapi import Fred
from sqlalchemy import create_engine, text
import os

try:
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable
    _conn = BaseHook.get_connection("marketpulse_postgres")
    DB_URL = f"postgresql+psycopg2://{_conn.login}:{_conn.password}@{_conn.host}:{_conn.port}/{_conn.schema}"
    FRED_API_KEY = Variable.get("FRED_API_KEY")
except Exception:
    from dotenv import load_dotenv
    load_dotenv()
    DB_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    FRED_API_KEY = os.getenv("FRED_API_KEY")

FRED_SERIES = {
    'FEDFUNDS': 'Federal Funds Rate',
    'CPIAUCSL': 'Consumer Price Index',
    'UNRATE':   'Unemployment Rate',
    'GS10':     'Ten Year Treasury Yield',
    'GDPC1':    'Real GDP'
}

def create_table(engine):
    with engine.begin() as conn:
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

def get_last_date(series_id, engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT MAX(date) FROM bronze_macro_indicators WHERE series_id = :series_id
        """), {"series_id": series_id})
        return result.scalar()

def ingest_series(series_id, series_name, fred, engine):
    last_date = get_last_date(series_id, engine)
    start = str(last_date) if last_date else "2019-01-01"
    print(f"{series_id}: fetching from {start}")
    data = fred.get_series(series_id, observation_start=start)
    df = data.reset_index()
    df.columns = ['date', 'value']
    df['series_id'] = series_id
    df['series_name'] = series_name
    df = df[['series_id', 'series_name', 'date', 'value']]
    df.dropna(inplace=True)
    df = df[df['date'] > pd.Timestamp(last_date)] if last_date else df
    if len(df) == 0:
        print(f"{series_id}: no new rows to ingest")
    else:
        df.to_sql('bronze_macro_indicators', engine, if_exists='append', index=False)
        print(f"Ingested {len(df)} new rows for {series_id} - {series_name}")

def run():
    fred = Fred(api_key=FRED_API_KEY)
    engine = create_engine(DB_URL)
    create_table(engine)
    for series_id, series_name in FRED_SERIES.items():
        ingest_series(series_id, series_name, fred, engine)

if __name__ == "__main__":
    run()