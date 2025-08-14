# *-* coding: utf-8 *-*

from bs4 import BeautifulSoup
import requests
import pandas as pd
from utils.string_utils import trim_string

def get_sp500_comppanies_data(sp500_source:str) -> pd.DataFrame:
    """Fetches S&P 500 companies data from the given URL and returns it as a DataFrame."""
    response = requests.get(sp500_source)
    soup = BeautifulSoup(response.text, 'lxml')
    table = soup.find('table', {'id': 'constituents'})
    raw_header = (table
                  .tr
                  .text
                  )
    header = [col for col in raw_header.split('\n') if len(col) > 0]
    data = {h: [] for h in header}
    rows = table.find_all('tr')[1:]
    for row in rows:
        cols = row.find_all('td')
        for h, col in zip(header, cols):
            col_text = col.text.strip()
            data[h].append(None if len(col_text) < 1 else col_text)
    df = pd.DataFrame(data)
    return df

def get_sp500_tickers(url: str) -> list:
    """Fetches S&P 500 tickers from the given URL and returns a list of ticker symbols."""
    df = get_sp500_comppanies_data(url)
    return df['Symbol'].tolist()