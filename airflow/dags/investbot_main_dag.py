import config
import logging
import pandas as pd
import pendulum
from datetime import timedelta
from time import sleep
from sqlalchemy.exc import IntegrityError
from database_mngmnt import (
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
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, task

postgres_hook = PostgresHook("investbot-db")
db_engine = get_engine(postgres_hook.get_uri())


@task
def _database_setup():
    create_price_table(db_engine)
    create_sp500_companies_table(db_engine)
    create_sp500_changes_table(db_engine)


@task
def _sp500_data():
    get_sp500_companies_data(
        config.SP_500_URL,
        config.LAST_MODIFIED_SP500_DATE_FILE_PATH,
        tables_ids=["constituents", "changes"],
        db_uri=postgres_hook.get_uri(),
    )


@task
def _missing_date_ranges():
    start_date, end_date = date_range(months=config.SP500_STOCK_PRICE_RANGE)
    index_composition_stored_data = get_missing_price_ranges(
        start_date, end_date, db_engine
    )
    batches_dict = group_tickers_by_dates_range(index_composition_stored_data)
    batches_list = []
    for (start, end), tickers in batches_dict.items():
        batches_list.append({"start_date": start, "end_date": end, "tickers": tickers})

    logging.info(f"Found {len(batches_list)} batches to process.")
    return batches_list  # <-- Retorna a lista


@task
def _fettch_data(batches_list: list) -> list:  # <-- Recebe a lista
    all_price_data = []  # <-- Acumulador principal FORA do loop

    if not batches_list:
        logging.info("No batches received. Skipping fetch.")
        return []

    for batch in batches_list:  # <-- Itera sobre a lista
        range_start_date = batch["start_date"]  # <-- Pega do dict
        range_end_date = batch["end_date"]  # <-- Pega do dict
        tickers = batch["tickers"]  # <-- Pega do dict

        logging.info(
            f"Fetching data for {', '.join(tickers)} for {range_start_date} to {range_end_date}..."
        )
        sub_batches_size = 50
        data = []  # Acumulador do sub-batch

        for i in range(0, len(tickers), sub_batches_size):
            sub_batch = tickers[i : i + sub_batches_size]
            logging.info(f"Fetching sub-batch of {len(sub_batch)} tickers...")
            current_sub_batch_data = fetch_historical_data(
                sub_batch, range_start_date, range_end_date
            )
            if current_sub_batch_data is not None and not current_sub_batch_data.empty:
                data.append(current_sub_batch_data)
            sleep(5)

        try:
            price_data_df = pd.concat(data)
            price_dict = price_data_df.to_dict(orient="records")
            all_price_data.extend(price_dict)  # <-- Acumula no principal
        except ValueError or AttributeError:
            logging.warning(
                f"No data fetched for {', '.join(tickers)} for {range_start_date} to {range_end_date}. Skipping..."
            )

    return all_price_data  # <-- Retorna TUDO no final


@task
def _load_fetched_data(price_dict: list):
    if not price_dict:
        logging.warning("No price data received to load. Skipping.")
        return

    try:
        load_data_to_db(price_dict, "stock_prices", db_engine, mode="append")
    except IntegrityError:
        tickers = set(item["ticker"] for item in price_dict)
        logging.warning(
            f"Data for {', '.join(tickers)} already exists in the database. Skipping..."
        )
    except Exception as e:
        logging.error(f"Error loading data: {e}")


default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="investbot-main-dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["investbot", "etl"],
    default_args=default_args,
) as dag:
    setup_task = _database_setup()
    sp500_task = _sp500_data()
    missing_ranges = _missing_date_ranges()

    fetched_data = _fettch_data(missing_ranges)
    load_task = _load_fetched_data(fetched_data)

    setup_task >> sp500_task >> missing_ranges
