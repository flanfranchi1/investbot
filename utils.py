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


def parse_wikipedia_table(table_element: BeautifulSoup) -> dict:
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

        cells = row.find_all(['th', 'td'])
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

    adj_final_headers = list(map(snake_case, final_headers))
    if not final_headers:
        return list_to_dict(extracted_data, headers=adj_final_headers)
    else:

        num_cols = len(final_headers)
        clean_data = [row for row in extracted_data if len(row) == num_cols]
        return list_to_dict(clean_data, adj_final_headers)
    
def list_to_dict(data:list[list], headers:list) -> list[dict]:
    """Converts a list of lists to a list of dictionaries using the provided headers."""
    dict_list = []
    for row in data:
        if len(row) == len(headers):
            row_dict = {headers[i]: row[i] for i in range(len(headers))}
            dict_list.append(row_dict)
        else:
            logging.warning(f"Row length {len(row)} does not match headers length {len(headers)}. Skipping row: {row}")
    return dict_list