# *-* coding: utf-8 -*-
import logging
import os
import requests
import sqlalchemy as db
import time
from dotenv import load_dotenv
from sourcing import get_sp500_tickers
from database import get_engine, create_price_table, create_sp500_table
from sqlalchemy.exc import IntegrityError
from datetime import datetime
logging.basicConfig(
    level=logging.INFO,
    # The format of the log message
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl.log"),
        logging.StreamHandler()
    ]
)

load_dotenv()
SP_500_URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'


def fetch_daily_data(ticker: str) -> dict:
    """Fetches daily time series data for a given stock ticker."""

    API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
    URL = (f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY'
           f'&symbol={ticker}'
           f'&apikey={API_KEY}'
           f'&outputsize=compact')  # Use 'full' for up to 20 years of data

    try:
        response = requests.get(URL)
        response.raise_for_status()  # Raises an exception for bad responses (4xx or 5xx)
        data = response.json()

        # Check for API-specific error messages
        if "Error Message" in data or 'Time Series (Daily)' not in data:
            logging.warning(
                f"API issue for {ticker}: {data.get('Note', data)}")
            return None
        return data

    except requests.exceptions.RequestException as e:
        logging.error(f"Network error fetching {ticker}: {e}")
        return None


def save_data_to_db(data: dict, ticker: str, engine: db.Engine) -> None:
    """Parses and saves stock data into the database."""
    connection = engine.connect()

    for date_str, values in data.items():
        price_date = datetime.strptime(date_str, '%Y-%m-%d').date()

        insert_query = """
        INSERT INTO stock_prices (ticker, price_date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        try:
            connection.execute(insert_query, (
                ticker,
                price_date,
                float(values['1. open']),
                float(values['2. high']),
                float(values['3. low']),
                float(values['4. close']),
                int(values['5. volume'])
            ))
        except IntegrityError:
            pass
        except Exception as e:
            logging.error(
                f"Error saving data for {ticker} on {date_str}: {e} ({e})")

    connection.close()
    print(f"Successfully saved data for {ticker}.")


# --- Main execution block ---
if __name__ == "__main__":
    db_engine = get_engine()
    create_price_table(db_engine)
    create_sp500_table(db_engine)
    sp500_data = get_sp500_tickers(SP_500_URL)
    if sp500_data:
        logging.info(f"Fetched {len(sp500_data)} S&P 500 tickers.")
        all_tickers = sp500_data
        tickers_to_fetch = all_tickers[:15]
        for ticker in tickers_to_fetch:
            logging.info(f"Fetching data for {ticker}...")
            price_data = fetch_daily_data(ticker)
            if price_data and 'Time Series (Daily)' in price_data:
                daily_data = price_data['Time Series (Daily)']
                save_data_to_db(daily_data, ticker, db_engine)
            else:
                logging.error(f"Failed to fetch data for {ticker}. Skipping...")
            time.sleep(1)
