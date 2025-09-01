# *-* coding: utf-8 -*-

import logging
import pandas as pd
from data_sourcing import get_sp500_tickers, get_sp500_companies_data, fetch_historical_data
from database import get_engine, create_price_table, create_sp500_table
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from utils import date_range, snake_case
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    # The format of the log message
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl.log"),
        logging.StreamHandler()
    ]
)

SP_500_URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
BASE_DIR = Path(__file__).resolve().parent
RAW_DATA_DIR = BASE_DIR / 'data' / 'raw'
SQLITE_DIR = BASE_DIR / 'data' / 'sqlite' 

# --- Main execution block ---
if __name__ == "__main__":
    start_date, end_date = date_range(months=12, delay=-1) 
    db_engine = get_engine()
    create_price_table(db_engine)
    create_sp500_table(db_engine)
    sp500_data = get_sp500_tickers(get_sp500_companies_data(SP_500_URL))
    if sp500_data:
        logging.info(f"Fetched {len(sp500_data)} S&P 500 tickers.")
        for ticker in sp500_data:
            logging.info(f"Fetching data for {ticker}...")
            price_data_df = fetch_historical_data(
                ticker,
                start_date,
                end_date
            )
            if (isinstance(price_data_df, pd.DataFrame) and not price_data_df.empty) or price_data_df is not None:
                price_data_df['ticker'] = ticker
                adj_price_data_df = price_data_df.droplevel(axis=1, level=1)
                adj_columns = map(snake_case, adj_price_data_df.columns)
                adj_price_data_df.columns = adj_columns
                try:
                    adj_price_data_df.to_sql(
                        'stock_prices',
                        con=db_engine,
                        if_exists='append',
                        index=True
                    )
                except Exception as e:
                    logging.error(f"Failed to fetch data for {ticker}: {e} Skipping...")
