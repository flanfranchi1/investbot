# (*-coding: utf-8 -*)

import sqlalchemy as db
import logging

from pathlib import Path


def get_engine(db_url: Path) -> db.Engine:
    """Creates a database engine instance."""
    engine = db.create_engine(db_url)
    return engine


def create_sp500_companies_table(engine):
    """Creates the sp500_companies table if it doesn't exist."""
    metadata = db.MetaData()

    db.Table(
        "sp500_companies",
        metadata,
        db.Column("symbol", db.String(5), primary_key=True),
        db.Column("security", db.String, nullable=True),
        db.Column("gics_sector", db.String, nullable=True),
        db.Column("gics_sub_industry", db.String, nullable=True),
        db.Column("headquarters_location", db.String, nullable=True),
        db.Column("date_added", db.String, nullable=True),
        db.Column("cik", db.String, nullable=True),
        db.Column("founded", db.String, nullable=True),
        db.Column("registry_date", db.Date, nullable=True),
        db.UniqueConstraint("symbol", "date_added", name="uix_symbol_date"),
    )
    metadata.create_all(engine)
    logging.info("Table 'sp500_companies' is ready.")


def create_sp500_changes_table(engine):
    """Creates the sp500_changes table if it doesn't exist."""
    metadata = db.MetaData()

    db.Table(
        "sp500_changes",
        metadata,
        db.Column("id", db.Integer, primary_key=True, autoincrement=True),
        db.Column("effective_date", db.String),
        db.Column("added_ticker", db.String(5), nullable=True),
        db.Column("added_security", db.String, nullable=True),
        db.Column("removed_ticker", db.String, nullable=True),
        db.Column("removed_security", db.String, nullable=True),
        db.Column("date_added", db.Date, nullable=True),
        db.Column("reason", db.String, nullable=True),
        db.UniqueConstraint("id", "removed_ticker", name="uix_sdate_tickers"),
    )
    metadata.create_all(engine)
    logging.info("Table 'sp500_cohanges' is ready.")


def create_price_table(engine: db.Engine) -> None:
    """Creates the stock_prices table if it doesn't exist."""
    metadata = db.MetaData()

    db.Table(
        "stock_prices",
        metadata,
        db.Column("id", db.Integer, primary_key=True),
        db.Column("ticker", db.String(10), nullable=True),
        db.Column("date", db.Date, nullable=True),
        db.Column("open", db.Float, nullable=True),
        db.Column("high", db.Float, nullable=True),
        db.Column("low", db.Float, nullable=True),
        db.Column("close", db.Float, nullable=True),
        db.Column("volume", db.Integer, nullable=True),
        db.Column("created_at", db.TIMESTAMP, server_default=db.func.now()),
        db.UniqueConstraint("ticker", "date", name="uix_ticker_date"),
    )

    metadata.create_all(engine)
    logging.info("Table 'stock_prices' is ready.")


def load_data_to_db(
    data: list[dict], table_name: str, engine: db.Engine, mode: str = "append"
) -> None:
    """Loads data into the specified database table."""
    metadata = db.MetaData()
    table = db.Table(table_name, metadata, autoload_with=engine)
    with engine.connect() as connection:
        with connection.begin() as transaction:
            if mode == "replace":
                try:
                    connection.execute(table.delete())
                    logging.info(f"Cleared existing data from '{table_name}' table.")
                except Exception as e:
                    transaction.rollback()
                    logging.error(f"Error while clearing data from '{table_name}': {e}")
                    return
            else:
                try:
                    connection.execute(table.insert(), data)
                    logging.info(
                        f"Inserted {len(data)} records into '{table_name}' table."
                    )
                except db.exc.IntegrityError as e:
                    logging.error(
                        f"Integrity error while inserting data into '{table_name}': {e}"
                    )
                except Exception as e:
                    logging.error(
                        f"Error while inserting data into '{table_name}': {e}"
                    )
