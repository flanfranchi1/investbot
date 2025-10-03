# *-* coding: utf-8 -*-

import config
import logging
from sqlalchemy.exc import IntegrityError
from database import (
    get_engine,
    create_price_table,
    create_sp500_companies_table,
    create_sp500_changes_table,
    load_data_to_db,
)
from data_sourcing import (
    get_sp500_companies_data,
    fetch_historical_data,
)
from utils import date_range, group_tickers_by_dates_range
from transformations import get_missing_price_ranges

logging.basicConfig(
    level=logging.INFO,
    # The format of the log message
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl.log"), logging.StreamHandler()],
)


# --- Main execution block ---
if __name__ == "__main__":
    start_date, end_date = date_range(months=config.SP500_STOCK_PRICE_RANGE, delay=-1)
    db_engine = get_engine(config.POSTGRES_URL)
    create_price_table(db_engine)
    create_sp500_companies_table(db_engine)
    create_sp500_changes_table(db_engine)
    get_sp500_companies_data(
        config.SP_500_URL,
        config.LAST_MODIFIED_SP500_DATE_FILE_PATH,
        tables_ids=["constituents", "changes"],
    )
    while True:
        index_composition_stored_data = get_missing_price_ranges(
            start_date, end_date, db_engine
        )
        batches = group_tickers_by_dates_range(index_composition_stored_data)
        for range, tickers in batches.items():
            start_date, end_date = range
            logging.info(f"Fetching data for {', '.join(tickers)} for {range}...")
            price_data_df = fetch_historical_data(tickers, start_date, end_date)
            try:
                price_dict = price_data_df.to_dict(orient="records")
            except AttributeError:
                logging.warning(
                    f"No data fetched for {', '.join(tickers)} for {range}. Skipping..."
                )
                continue
            else:
                try:
                    load_data_to_db(
                        price_dict, "stock_prices", db_engine, mode="append"
                    )
                except IntegrityError:
                    logging.warning(
                        f"Data for {', '.join(tickers)} for {range} already exists in the database. Skipping..."
                    )
                    break
