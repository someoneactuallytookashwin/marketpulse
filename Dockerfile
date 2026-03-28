FROM apache/airflow:2.8.1
RUN pip install "multitasking==0.0.9" "yfinance==0.2.28" pandas pandas-datareader sqlalchemy psycopg2-binary python-dotenv fredapi