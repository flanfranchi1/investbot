# *-* coding: utf-8 *-*

import logging
import pandas as pd
import requests
from typing import Any

def get_sp500_companies_data(url_or_body: any) -> pd.DataFrame:
    """
    Fetches S&P 500 companies data from a URL and returns it as a DataFrame."""
    df = pd.read_html(url_or_body, header=0, flavor='bs4')[0]
    logging.info(
        "Successfully fetched and parsed S&P 500 table from Wikipedia.")
    renamed_cols = map(
        lambda col: col.lower().replace(' ', '_').replace('-', '_'),
        df
        .columns
        .to_list()
    )
    df.columns = renamed_cols
    df['registry_date'] = pd.to_datetime('today').date()
    return df


def get_sp500_tickers(df: pd.DataFrame) -> list:
    """extracts and returns a list of S&P 500 ticker symbols from the DataFrame."""
    return df['symbol'].tolist()

def save_sp500_companies_to_db(df: pd.DataFrame, engine: Any) -> None:
    """
    Saves the S&P 500 companies DataFrame to the database.
    If the table already exists, it will be replaced.
    """
    df.to_sql('sp500_companies', con=engine, if_exists='replace', index=False)
    logging.info("S&P 500 companies data saved to database.")

def fetch_daily_data(ticker: str, api_key:str) -> dict:
    """Fetches daily time series data for a given stock ticker."""

    
    URL = (f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY'
           f'&symbol={ticker}'
           f'&apikey={api_key}'
           f'&outputsize=full')

    try:
        response = requests.get(URL)
        response.raise_for_status()
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

