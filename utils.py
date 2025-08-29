# *-* coding: utf-8 -*-

import json
import logging
import requests
from datetime import datetime, timedelta
from typing import Any, List, Iterator

from pathlib import Path

def filter_out_files(path: str, target:list) -> list:
    """Return a list of all target companies for the current run."""
    assets_path = Path(path)
    if not assets_path.exists():
        logging.error(f"Assets path {path} does not exist.")
        return target

    asset_files = [f.name for f in assets_path.glob("**/*.json")]
    logging.info(f"Found {len(asset_files)} asset files in {path}.")
    return [t for t in target if f"{t}.json" not in asset_files]

def save_as_json(data: dict, ticker: str, path: str) -> None:
    """Saves the fetched data as a JSON file."""
    try:
        assets_path = Path(path)
        assets_path.mkdir(parents=True, exist_ok=True)
        file_path = assets_path / f"{ticker}.json"
        with open(file_path, 'w') as f:
            json.dumps(data, f, indent=4)
        logging.info(f"Data for {ticker} saved to {file_path}.")
    except Exception as e:
        logging.error(f"Error saving data for {ticker}: {e}")

def date_range(months: int, delay:int=0) -> tuple:
    """Returns start and end dates for the given number of months."""
    end_date = datetime.now() + timedelta(days=delay)
    start_date = end_date - timedelta(days=30 * months)
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

snake_case = lambda s: (
    s.lower()
    .replace(' ', '_')
    .replace('-', '_')
    .replace('.', '')
)

def chunk_list(data: List[Any], chunk_size: int) -> Iterator[List[Any]]:
    """Yield successive n-sized chunks from data."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]