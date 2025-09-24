# *-* coding: utf-8 -*-

import config
import logging
import pandas as pd
from database import *
from data_sourcing import (
    get_sp500_companies_data,
    fetch_historical_data,
    converting_list_of_dicts_to_dataframe,
)
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from utils import (
    date_range,
    group_tickers_by_dates_range,
    save_dates_range_dict_as_json,
)
from transformations import get_missing_price_ranges
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    # The format of the log message
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl.log"), logging.StreamHandler()],
)


# --- Main execution block ---
if __name__ == "__main__":
    start_date, end_date = date_range(months=config.SP500_STOCK_PRICE_RANGE, delay=-1)
    db_engine = get_engine(config.SQLITE_DB_PATH)
    create_price_table(db_engine)
    create_sp500_companies_table(db_engine)
    create_sp500_changes_table(db_engine)
    get_sp500_companies_data(
        config.SP_500_URL,
        config.LAST_MODIFIED_SP500_DATE_FILE_PATH,
        tables_ids=["constituents", "changes"],
    )
    index_composition_stored_data = get_missing_price_ranges(
        start_date, end_date, db_engine
    )
    if index_composition_stored_data:
        batches = group_tickers_by_dates_range(index_composition_stored_data)
        # logging.info(f"Fetched {len(index_composition_stored_data)} S&P 500 tickers.")
        df_list = []
        for range, tickers in batches.items():
            start_date, end_date = range
            logging.info(f"Fetching data for {tickers}...")
            price_data_df = fetch_historical_data(tickers, start_date, end_date)
            df_list.append(price_data_df)
        try:
            stacked_chuncks_df = pd.concat(df_list)
        except ValueError as v:
            logging.error(f"No data fetched: {v}")
            stacked_chuncks_df = None
        save_dates_range_dict_as_json(batches, config.missing_dates_json_path)
        if (
            isinstance(stacked_chuncks_df, pd.DataFrame)
            and not stacked_chuncks_df.empty
        ) or stacked_chuncks_df is not None:
            price_dict = stacked_chuncks_df.to_dict(orient="records")
            try:
                load_data_to_db(price_dict, "stock_prices", db_engine, mode="append")
                logging.info(f"Data for {tickers} loaded successfully.")
            except Exception as e:
                logging.error(f"Failed to fetch data for {tickers}: {e} Skipping...")
