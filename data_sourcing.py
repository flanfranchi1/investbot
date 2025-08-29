# *-* coding: utf-8 *-*

import logging
import config 
import polars as pl 
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from database import get_engine, get_sp500_companies
from utils import snake_case
from typing import Any
from pathlib import PosixPath

def get_sp500_companies_data(sp500_source:str, last_modified_file:PosixPath|str=config.LAST_MODIFIED_SP500_DATE_FILE_PATH) -> pl.DataFrame|None:
    """Fetches S&P 500 companies data from the given URL and returns it as a DataFrame."""
    last_modified_file.parent.mkdir(exist_ok=True)
    try:
        last_modified = last_modified_file.read_text().strip()
    except FileNotFoundError:
        last_modified = ''
    headers = {
        'User-Agent': 'InvestBot/1.0 (https://github.com/flanfranchi1/investbot; felanfranchi@gmail.com',
        'If-Modified-Since': last_modified
    }
    response = requests.get(sp500_source, headers=headers)
    if response.status_code == 304:
        logging.info("S&P 500 data not modified since last fetch.")
        engine = get_engine()
        current_data = get_sp500_companies(engine )
        if current_data is not None:
            current_data_df = pl.DataFrame(current_data)
            return current_data_df 
        
    elif response.status_code != 200:
        logging.error(f"Failed to fetch S&P 500 data. Status code: {response.status_code}")
        return None
    else:
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
        df = pl.DataFrame(data)
        with open(last_modified_file, 'w') as f:
            f.write(response.headers.get('Last-Modified'))
        return df

def get_sp500_tickers(df: pl.DataFrame) -> list:
    """extracts and returns a list of S&P 500 ticker symbols from the DataFrame."""
    lst = df.to_series().to_list()
    return lst

def fetch_historical_data(ticker: str, start_date: str, end_date: str) -> pl.DataFrame | None:
    "Fetches historical stock data from Yahoo Finance."
    try:
        data = yf.download(ticker, start=start_date, end=end_date)

        if data.empty:
            logging.warning(f"No data found for {ticker} in the given date range.")
            return None

        logging.info(f"Successfully fetched {len(data)} records for {ticker}.")
        pl_data = pl.from_pandas(data)
        return pl_data
    except Exception as e:
        logging.error(f"An error occurred while fetching data for {ticker}: {e}")
