"""
YFinance ingestion placeholder.

Later, wire this into Airflow tasks and persist to your chosen warehouse/storage.
"""

from __future__ import annotations

import os
from datetime import date


def ingest_yfinance(symbols: list[str], start: date, end: date) -> None:
    """
    Ingest price data for the provided `symbols` between `start` and `end`.

    TODO: implement actual downloading + persistence.
    """

    # Deliberately no external imports yet to keep this as a structural placeholder.
    # When you implement:
    #   pip install yfinance
    #   import yfinance as yf
    #   df = yf.download(symbols, start=start, end=end)
    #   save df to warehouse/storage
    _ = (symbols, start, end)


def main() -> None:
    symbols_env = os.getenv("YFINANCE_SYMBOLS", "AAPL,MSFT")
    symbols = [s.strip() for s in symbols_env.split(",") if s.strip()]

    # Placeholder date range
    start = date(2020, 1, 1)
    end = date.today()

    ingest_yfinance(symbols=symbols, start=start, end=end)


if __name__ == "__main__":
    main()

