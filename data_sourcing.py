# *-* coding: utf-8 *-*

import config
import logging
import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from database import get_engine, load_data_to_db
from pandas_market_calendars import get_calendar
from utils import snake_case, parse_wikipedia_table
from pathlib import Path
from typing import Any


def get_sp500_companies_data(
    sp500_source: str,
    last_change_date_file: Path,
    tables_ids: list[str] = ["constituents", "changes"],
) -> pd.DataFrame:
    """Fetches S&P 500 companies data from the given URL and returns it as a DataFrame."""
    (last_change_date_file.parent.mkdir(parents=True, exist_ok=True))
    try:
        last_stored_date = last_change_date_file.read_text(encoding="utf-8").strip()
    except Exception as e:
        last_stored_date = ""
        logging.warning(f"Could not read last modified date file: {e}")

    headers = {
        "User-Agent": "InvestBot/1.0 (https://github.com/flanfranchi1/investbot; felanfranchi@gmail.com)",
        "If-Modified-Since": last_stored_date,
    }
    response = requests.get(sp500_source, headers=headers)
    if response.status_code == 304:
        logging.info("No updates to S&P 500 data since last fetch.")
    elif response.status_code != 200:
        logging.error(f"Failed to fetch S&P 500 data: HTTP {response.status_code}")
    else:
        soup = BeautifulSoup(response.text, "lxml")
        last_modified = response.headers.get("Last-Modified", "")
        try:
            with last_change_date_file.open("w", encoding="utf-8") as file:
                file.write(last_modified)
        except Exception as e:
            logging.error(
                f"Could not write last modified date ({last_modified}) to file: {e}"
            )
        for table_id in tables_ids:
            table = soup.find("table", {"id": table_id})
            data = parse_wikipedia_table(table)
            db_engine = get_engine(config.POSTGRES_URL)
            table_name = (
                f"sp500_{'companies' if table_id == 'constituents' else 'changes'}"
            )
            load_data_to_db(data, table_name, db_engine, mode="overwrite")


def fetch_historical_data(
    ticker: str | list[str], start_date: str, end_date: str
) -> pd.DataFrame | None:
    "Fetches historical stock data from Yahoo Finance."
    try:
        data = yf.download(ticker, start=start_date, end=end_date)

        if data.empty:
            logging.warning(f"No data found for {ticker} in the given date range.")
            return None

        logging.info(f"Successfully fetched {len(data)} records for {ticker}.")
        adj_data = (
            data.stack(level=1).rename_axis(index=["Date", "Ticker"]).reset_index()
        )
        adj_columns = [snake_case(col) for col in adj_data.columns]
        adj_data.columns = adj_columns
        return adj_data
    except Exception as e:
        logging.error(f"An error occurred while fetching data for {ticker}: {e}")


def converting_list_of_dicts_to_dataframe(data: list[dict[str, Any]]) -> pd.DataFrame:
    """Converts a list of dictionaries to a Pandas DataFrame."""
    if not data:
        df = pd.DataFrame()
    else:
        df = pd.DataFrame(data)
        df["first_date"] = pd.to_datetime(df["first_date"], errors="coerce")
        df["last_date"] = pd.to_datetime(df["last_date"], errors="coerce")
    return df


def get_market_working_days(start_date: str, end_date: str) -> pd.DatetimeIndex:
    """Returns a list of market working days between start_date and end_date."""
    nyse_calendar = get_calendar("NYSE")
    valid_dates = nyse_calendar.valid_days(start_date=start_date, end_date=end_date)
    return valid_dates
