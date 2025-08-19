# *-* coding: utf-8 -*-

import config
import logging
import os
import pandas as pd
from pathlib import Path
import sqlalchemy as db
import time
from dotenv import load_dotenv
from sourcing import get_sp500_tickers, get_sp500_companies_data, fetch_daily_data
from database import get_engine, create_price_table, create_sp500_table
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from pathlib import Path
from utils import filter_out_files, save_as_json

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
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
BASE_DIR = Path(__file__).resolve().parent
RAW_DATA_DIR = BASE_DIR / 'data' / 'raw'
SQLITE_DIR = BASE_DIR / 'data' / 'sqlite' 

# --- Main execution block ---
if __name__ == "__main__":
    db_engine = get_engine()
    create_price_table(db_engine)
    create_sp500_table(db_engine)
    sp500_data = get_sp500_tickers(get_sp500_companies_data(SP_500_URL))
    if sp500_data:
        logging.info(f"Fetched {len(sp500_data)} S&P 500 tickers.")
        tickers_to_fetch = filter_out_files(RAW_DATA_DIR, sp500_data)
        for ticker in tickers_to_fetch:
            logging.info(f"Fetching data for {ticker}...")
            price_data = fetch_daily_data(ticker, API_KEY)
            if price_data and 'Time Series (Daily)' in price_data:
                daily_data = price_data['Time Series (Daily)']
                save_as_json(price_data, ticker, RAW_DATA_DIR)
            else:
                logging.error(f"Failed to fetch data for {ticker}. Skipping...")
            time.sleep(1)
