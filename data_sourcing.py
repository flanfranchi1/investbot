# *-* coding: utf-8 *-*

import logging
import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from database import get_engine
from utils import snake_case
from typing import Any


def get_sp500_companies_data(sp500_source: str) -> pd.DataFrame:
    """Fetches S&P 500 companies data from the given URL and returns it as a DataFrame."""
    headers = {
        'User-Agent': 'InvestBot/1.0 (https://github.com/flanfranchi1/investbot; felanfranchi@gmail.com)'
    }
    response = requests.get(sp500_source, headers=headers)
    soup = BeautifulSoup(response.text, 'lxml')
    table = soup.find('table', {'id': 'constituents'})
    raw_header = (table
                  .tr
                  .text
                  )
    header = [col for col in raw_header.split('\n') if len(col) > 0]
    data = {h: [] for h in header}
    rows = table.find_all('tr')[1:]
    for row in rows:
        cols = row.find_all('td')
        for h, col in zip(header, cols):
            col_text = col.text.strip()
            data[h].append(None if len(col_text) < 1 else col_text)
    df = pd.DataFrame(data)
    df.columns = map(snake_case, df.columns)
    return df


def get_sp500_tickers(df: pd.DataFrame) -> list:
    """extracts and returns a list of S&P 500 ticker symbols from the DataFrame."""
    sql_query_to_avoid_duplicates = "SELECT DISTINCT ticker FROM STOCK_PRICES"
    engine = get_engine()
    existing_tickers_df = pd.read_sql(
        sql_query_to_avoid_duplicates, con=engine)
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
    return adj_tickers_df['symbol'].tolist()


def fetch_historical_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame | None:
    "Fetches historical stock data from Yahoo Finance."
    try:
        data = yf.download(ticker, start=start_date, end=end_date)

        if data.empty:
            logging.warning(
                f"No data found for {ticker} in the given date range.")
            return None

        logging.info(f"Successfully fetched {len(data)} records for {ticker}.")
        data['ticker'] = ticker
        adj_price_data_df = data.droplevel(axis=1, level=1)
        adj_columns = map(snake_case, adj_price_data_df.columns)
        adj_price_data_df.columns = adj_columns
        return data
    except Exception as e:
        logging.error(
            f"An error occurred while fetching data for {ticker}: {e}")
