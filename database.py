# (*-coding: utf-8 -*)

import sqlalchemy as db
import logging
from datetime import datetime
from typing import List, Dict, Set


def get_engine() -> db.Engine:
    """Creates a database engine instance."""
    engine = db.create_engine('sqlite:///data/sqlite/stock_data.db')
    return engine


def create_sp500_table(engine):
    """Creates the sp500_companies table if it doesn't exist."""
    metadata = db.MetaData()

    db.Table('sp500_companies', metadata,
             db.Column('symbol', db.String(5), primary_key=True),
             db.Column('security', db.String, nullable=False),
             db.Column('gics_sector', db.String, nullable=False),
             db.Column('gics_sub_industry', db.String, nullable=False),
             db.Column('headquarters_location', db.String, nullable=False),
             db.Column('data_added', db.Date, nullable=False),
             db.Column('cik', db.String, nullable=True),
             db.Column('founded', db.Integer, nullable=True),
             db.Column('registry_date', db.Date, nullable=False),
             db.UniqueConstraint('symbol', 'data_added',
                                 name='uix_symbol_date')
             )
    metadata.create_all(engine)
    logging.info("Table 'sp500_companies' is ready.")


def create_price_table(engine: db.Engine) -> None:
    """Creates the stock_prices table if it doesn't exist."""
    metadata = db.MetaData()

    db.Table('stock_prices', metadata,
             db.Column('id', db.Integer, primary_key=True),
             db.Column('ticker', db.String(10), nullable=False),
             db.Column('date', db.Date, nullable=False),
             db.Column('open', db.Float, nullable=False),
             db.Column('high', db.Float, nullable=False),
             db.Column('low', db.Float, nullable=False),
             db.Column('close', db.Float, nullable=False),
             db.Column('volume', db.Integer, nullable=False),
             db.UniqueConstraint('ticker', 'date',
                                 name='uix_ticker_date')
             )

    metadata.create_all(engine)
    logging.info("Table 'stock_prices' is ready.")



def get_sp500_companies(engine: db.Engine) -> List[str]:
    """Fetches all records from the sp500_companies table."""
    with engine.connect() as connection:
        query = db.text("SELECT * FROM sp500_companies")
        result = connection.execute(query)
        companies = [t[1] for t in result.fetchall()]
    return companies 

def find_all_missing_dates(
    engine: db.Engine,
    required_dates: List[datetime]
) -> Dict[str, List[datetime]]:
    """
    Identifies all missing dates for every ticker in the stock_prices table.

    Args:
        engine: The SQLAlchemy engine instance.
        required_dates: A complete list of market dates (as datetime objects)
                        that should exist in the database.

    Returns:
        A dictionary where keys are ticker symbols and values are lists of
        the missing datetime objects for that ticker.
    """
    missing_data_map = {}
    
    required_dates_set: Set[datetime.date] = {dt.date() for dt in required_dates}

    with engine.connect() as connection:
        tickers_query = db.text("SELECT DISTINCT ticker FROM stock_prices")
        tickers = [row[0] for row in connection.execute(tickers_query)]

        for ticker in tickers:
            dates_query = db.text("SELECT DISTINCT date FROM stock_prices WHERE ticker = :ticker and date != '0000-00-00'")
            
            result = connection.execute(dates_query, {"ticker": ticker})
            
            # Convert the query results into a set of date objects.
            existing_dates_set: Set[datetime.date] = set([datetime.strptime(row[0][:10], '%Y-%m-%d').date() for row in result])

            missing_dates_set = required_dates_set - set(existing_dates_set)

            if missing_dates_set:
                original_datetime_map = {dt.date(): dt for dt in required_dates}
                missing_datetimes = sorted(
                    [original_datetime_map[d] for d in missing_dates_set]
                )
                missing_data_map[ticker] = missing_datetimes

    return missing_data_map