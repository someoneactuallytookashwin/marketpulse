"""
FRED ingestion placeholder.

Later, wire this into Airflow tasks and persist to your chosen warehouse/storage.
"""

from __future__ import annotations

import os


def ingest_fred(series_ids: list[str]) -> None:
    """
    Ingest FRED time series for the given `series_ids`.

    TODO: implement actual API calling + persistence.
    """

    fred_api_key = os.getenv("FRED_API_KEY")
    if not fred_api_key or fred_api_key == "replace_me":
        raise RuntimeError(
            "Missing/placeholder FRED_API_KEY. Set it in your local environment (.env)."
        )

    # Placeholder: later you might use requests to call the FRED API and store results.
    _ = series_ids


def main() -> None:
    series_env = os.getenv("FRED_SERIES_IDS", "CPIAUCSL,GDP")
    series_ids = [s.strip() for s in series_env.split(",") if s.strip()]
    ingest_fred(series_ids=series_ids)


if __name__ == "__main__":
    main()

