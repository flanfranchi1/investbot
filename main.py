# *-* coding: utf-8 -*-

import logging
import config
import polars as pl
import pandas_market_calendars as mcal
from data_sourcing import get_sp500_tickers, get_sp500_companies_data, fetch_historical_data
from database import get_engine, create_price_table, create_sp500_table, find_all_missing_dates
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from utils import date_range, snake_case, chunk_list
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



# --- Main execution block ---
if __name__ == "__main__":
    start_date, end_date = date_range(months=12)
    trade_dates = list(
        mcal.get_calendar('NYSE')
        .valid_days(start_date=start_date, end_date=end_date)
        .to_pydatetime()
    )
    db_engine = get_engine()
    create_price_table(db_engine)
    create_sp500_table(db_engine)
    target_tickers_and_dates = find_all_missing_dates(db_engine, trade_dates)
    sp500_data = get_sp500_tickers(get_sp500_companies_data(config.SP_500_URL, config.LAST_MODIFIED_SP500_DATE_FILE_PATH))
    if sp500_data:
        logging.info(f"Fetched {len(sp500_data)} S&P 500 tickers.")
        for ticker_chunk in chunk_list(sp500_data, 50):
            logging.info(f"Fetching data for {', '.join(ticker_chunk)}...")
            price_data_df = fetch_historical_data(
                ticker_chunk,
                start_date,
                end_date
            )
            if (isinstance(price_data_df, pl.DataFrame) and not price_data_df.is_empty()) or price_data_df is not None:
                adj_price_data_df = (price_data_df
                    .stack(level=1)
                    .reset_index()
                    .rename(columns={'level_1': 'ticker', 0:'date'})
                )
                adj_columns = map(snake_case, adj_price_data_df.columns)
                adj_price_data_df.columns = adj_columns
                try:
                    adj_price_data_df.to_sql(
                        'stock_prices',
                        con=db_engine,
                        if_exists='append',
                        index=False
                    )
                except Exception as e:
                    logging.error(f"Failed to fetch data for {', '.join(ticker_chunk)}: {e} Skipping...")
