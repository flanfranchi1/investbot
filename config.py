# *-- coding: utf-8 --*
from pathlib import Path


SP_500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
POSTGRES_URL = (
    "postgresql+psycopg2://investbot_user:investbot_password@localhost/investbot_db"
)
BASE_DIR = Path(__file__).resolve().parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
MISSING_DATA_LOG_FOLDER = RAW_DATA_DIR / "missing_data"
WIKI_SP_500_UPDATED_AT_FILE_PATH = RAW_DATA_DIR / "sp500_wiki_last_updated.txt"
SQLITE_DIR = BASE_DIR / "data" / "sqlite"
SQLITE_DB_PATH = SQLITE_DIR / "stock_data.db"
LAST_MODIFIED_SP500_DATE_FILE_PATH = RAW_DATA_DIR / "sp500_last_modified.txt"
SQL_QUERY_DIR = BASE_DIR / "sql"
SP500_STOCK_PRICE_RANGE = 60  # months
