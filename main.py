# *-* coding: utf-8 -*-

import config
import logging
import pandas as pd
from database import *
from data_sourcing import get_sp500_companies_data, fetch_historical_data, converting_list_of_dicts_to_dataframe
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from utils import date_range
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
    start_date, end_date = date_range(months=12, delay=-1)
    db_engine = get_engine(config.SQLITE_DB_PATH)
    create_price_table(db_engine)
    create_sp500_companies_table(db_engine)
    create_sp500_changes_table(db_engine)
    get_sp500_companies_data(
        config.SP_500_URL,
        config.LAST_MODIFIED_SP500_DATE_FILE_PATH,
        tables_ids=['constituents', 'changes']
    )
    sp500_data = get_target_tickers(db_engine)
    tickers_and_dates_df = converting_list_of_dicts_to_dataframe(sp500_data)
    if sp500_data:
        tickers_chunk = len(sp500_data ) // 10 if len(sp500_data) > 10 else len(sp500_data)
        logging.info(f"Fetched {len(sp500_data)} S&P 500 tickers.")
        df_list = []
        for chunk in range(0, len(sp500_data), tickers_chunk):
            tickers = sp500_data[chunk: chunk + tickers_chunk]
            target_tickers = [item['ticker'] for item in tickers]
            logging.info(f"Fetching data for {tickers}...")
            price_data_df = fetch_historical_data(
                target_tickers,
                start_date,
                end_date
            )
            df_list.append(price_data_df)
        stacked_chuncks_df = pd.concat(df_list)
        if (isinstance(stacked_chuncks_df, pd.DataFrame) and not stacked_chuncks_df.empty) or stacked_chuncks_df is not None:
            tickers_and_dates_to_be_saved_df = (tickers_and_dates_df
                                                .merge(
                                                    stacked_chuncks_df,
                                                    how='left',
                                                    on='ticker'
                                                )
                                                .query('date < first_date or date > last_date')
                                                .drop(columns=['first_date', 'last_date'])
            ) 
            price_dict = tickers_and_dates_to_be_saved_df .to_dict(orient='records')
            try:
                load_data_to_db(
                    price_dict,
                    'stock_prices',
                    db_engine,
                    mode="append"
                )
                logging.info(f"Data for {tickers} loaded successfully.")
            except Exception as e:
                logging.error(
                    f"Failed to fetch data for {chunk}: {e} Skipping...")
