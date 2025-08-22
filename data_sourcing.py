# *-* coding: utf-8 *-*

import logging
import pandas as pd
import requests
import yfinance as yf
from database import get_engine
from utils import snake_case
from typing import Any

def get_sp500_companies_data(url_or_body: any) -> pd.DataFrame:
    """
    Fetches S&P 500 companies data from a URL and returns it as a DataFrame."""
    df = pd.read_html(url_or_body, header=0, flavor='bs4')[0]
    logging.info(
        "Successfully fetched and parsed S&P 500 table from Wikipedia.")
    renamed_cols = map(
        snake_case,
        df
        .columns
        .to_list()
    )
    df.columns = renamed_cols
    df['registry_date'] = pd.to_datetime('today').date()
    return df


def get_sp500_tickers(df: pd.DataFrame) -> list:
    """extracts and returns a list of S&P 500 ticker symbols from the DataFrame."""
    sql_query_to_avoid_duplicates = "SELECT DISTINCT ticker FROM STOCK_PRICES"
    engine = get_engine()
    existing_tickers_df = pd.read_sql(sql_query_to_avoid_duplicates, con=engine)
    tickers_df = df.merge(
        existing_tickers_df,
        left_on='symbol',
        right_on='ticker',
        how='left',
        indicator=True
    )
    adj_tickers_df = (
        tickers_df[tickers_df['_merge'] == 'left_only']
        .drop(columns=['_merge'])
    )
    return tickers_df['symbol'].tolist()

def fetch_historical_data(ticker: list|str, start_date: str, end_date: str) -> pd.DataFrame | None:
    "Fetches historical stock data from Yahoo Finance."
    try:
        data = yf.download(ticker, start=start_date, end=end_date)

        if data.empty:
            logging.warning(f"No data found for {ticker} in the given date range.")
            return None

        logging.info(f"Successfully fetched {len(data)} records for {ticker}.")
        return data
    except Exception as e:
        logging.error(f"An error occurred while fetching data for {ticker}: {e}")
