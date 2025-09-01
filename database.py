# (*-coding: utf-8 -*)

import sqlalchemy as db
import logging


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
