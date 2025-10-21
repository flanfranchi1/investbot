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
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _database_setup():
    db_engine = get_engine(config.POSTGRES_URL)
    create_price_table(db_engine)
    create_sp500_companies_table(db_engine)
    create_sp500_changes_table(db_engine)


def _sp500_data():
    get_sp500_companies_data(
        config.SP_500_URL,
        config.LAST_MODIFIED_SP500_DATE_FILE_PATH,
        tables_ids=["constituents", "changes"],
    )


def _missing_date_ranges():
    start_date, end_date = date_range(months=config.SP500_STOCK_PRICE_RANGE)
    engine = get_engine(config.POSTGRES_URL)
    index_composition_stored_data = get_missing_price_ranges(
        start_date, end_date, engine
    )
    batches = group_tickers_by_dates_range(index_composition_stored_data)
    return batches


def _fettch_data(missing_date_ranges: dict) -> list:
    for range, tickers in missing_date_ranges.items():
        range_start_date, range_end_date = range
        logging.info(f"Fetching data for {', '.join(tickers)} for {range}...")
        sub_batches_size = 50
        data = []
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
            return price_dict
        except ValueError or AttributeError:
            logging.warning(
                f"No data fetched for {', '.join(tickers)} for {date_range}. Skipping..."
            )
            continue


def load_fetched_data(price_dict: list):
    db_engine = get_engine(config.POSTGRES_URL)
    try:
        load_data_to_db(price_dict, "stock_prices", db_engine, mode="append")
    except IntegrityError:
        tickers = set(item["ticker"] for item in price_dict)
        logging.warning(
            f"Data for {', '.join(tickers)} for {date_range} already exists in the database. Skipping..."
        )


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
    database_setup = PythonOperator(
        task_id="database_setup", python_callable=_database_setup
    )
    sp500_data = PythonOperator(task_id="sp500_data", python_callable=_sp500_data)
    _missing_date_ranges = PythonOperator(
        task_id="missing_date_ranges", python_callable=_missing_date_ranges
    )
    fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=_fettch_data,
        op_args=["{{ ti.xcom_pull(task_ids='missing_date_ranges') }}"],
    )
    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_fetched_data,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_data') }}"],
    )
    database_setup >> sp500_data >> _missing_date_ranges >> fetch_data >> load_data
