# (*-coding: utf-8 -*)

import sqlalchemy as db


def get_engine() -> db.Engine:
    """Creates a database engine instance."""
    engine = db.create_engine('sqlite:///data/sqlite/stock_data.db')
    return engine


def create_price_table(engine: db.Engine) -> None:
    """Creates the stock_prices table if it doesn't exist."""
    metadata = db.MetaData()

    db.Table('stock_prices', metadata,
             db.Column('id', db.Integer, primary_key=True),
             db.Column('ticker', db.String(10), nullable=False),
             db.Column('price_date', db.Date, nullable=False),
             db.Column('open', db.Float, nullable=False),
             db.Column('high', db.Float, nullable=False),
             db.Column('low', db.Float, nullable=False),
             db.Column('close', db.Float, nullable=False),
             db.Column('volume', db.Integer, nullable=False),
             # Ensure a ticker/date combination is unique
             db.UniqueConstraint('ticker', 'price_date',
                                 name='uix_ticker_date')
             )

    metadata.create_all(engine)
    print("Table 'stock_prices' is ready.")
