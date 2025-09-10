# *-* coding: utf-8 -*-

import json
import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import Engine
from pathlib import Path
from datetime import datetime, timedelta

from pathlib import Path


def filter_out_files(path: str, target: list) -> list:
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


def date_range(months: int, delay: int = 0) -> tuple:
    """Returns start and end dates for the given number of months."""
    end_date = datetime.now() + timedelta(days=delay)
    start_date = end_date - timedelta(days=30 * months)
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


def snake_case(s): return (
    s.lower()
    .replace(' ', '_')
    .replace('-', '_')
    .replace('.', '')
)


def sql_query_to_dataframe(engine: Engine, query_file: Path) -> pd.DataFrame:
    """Executes a SQL query from a file and returns the result as a Polars DataFrame."""
    with Path.read_text(query_file) as query_file:
        with engine.connect() as connection:
            df = pd.read_sql(query_file, connection)
    return df


def parse_wikipedia_table(table_element: BeautifulSoup) -> pd.DataFrame:
    """
    Parses a Wikipedia table, handling complex headers with rowspan and colspan.

    Args:
        table_element: A BeautifulSoup object representing the <table> element.

    Returns:
        Polars DataFrame containing the table data.
    """
    all_rows = table_element.find_all('tr')
    header_rows = []
    data_rows = []
    header_ended = False
    for row in all_rows:
        is_header_row = len(row.find_all('th')) > 0
        if not header_ended and is_header_row:
            header_rows.append(row)
        else:
            header_ended = True
            if len(row.find_all('td')) > 0:
                data_rows.append(row)

    header_grid = []
    for r, row in enumerate(header_rows):
        while len(header_grid) <= r:
            header_grid.append([])

        cells = row.find_all(['th', 'td'])  # Inclui td para casos raros
        for cell in cells:
            c = 0
            while c < len(header_grid[r]) and header_grid[r][c] is not None:
                c += 1

            rowspan = int(cell.get('rowspan', 1))
            colspan = int(cell.get('colspan', 1))

            for i in range(rowspan):
                for j in range(colspan):
                    while len(header_grid) <= r + i:
                        header_grid.append([])
                    while len(header_grid[r + i]) <= c + j:
                        header_grid[r + i].append(None)

                    header_grid[r + i][c + j] = cell.text.strip()

    final_headers = []
    if header_grid:
        max_cols = max(len(row) for row in header_grid)
        for c in range(max_cols):
            column_texts = []
            for r in range(len(header_grid)):
                if c < len(header_grid[r]) and header_grid[r][c] is not None:
                    if header_grid[r][c] not in column_texts:
                        column_texts.append(header_grid[r][c])
            final_headers.append(" ".join(column_texts))

    extracted_data = []
    for row in data_rows:
        cells = row.find_all('td')
        extracted_data.append([cell.text.strip() for cell in cells])

    if not final_headers:
        return pd.DataFrame(extracted_data)
    else:

        num_cols = len(final_headers)
        clean_data = [row for row in extracted_data if len(row) == num_cols]
        return pd.DataFrame(clean_data, columns=final_headers)
