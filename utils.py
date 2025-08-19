# *-* coding: utf-8 -*-

import json
import logging
import requests

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
