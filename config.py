# *-- coding: utf-8 --*
from pathlib import Path

SP_500_URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
BASE_DIR = Path(__file__).resolve().parent
RAW_DATA_DIR = BASE_DIR / 'data' / 'raw'
SQLITE_DIR = BASE_DIR / 'data' / 'sqlite'
LAST_MODIFIED_SP500_DATE_FILE_PATH = RAW_DATA_DIR / 'sp500_last_modified.txt'