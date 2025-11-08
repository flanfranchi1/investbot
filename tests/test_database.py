import sqlalchemy as db
from database_mngmnt import (
    get_engine,
    create_sp500_companies_table,
    create_sp500_changes_table,
    create_price_table,
)


def test_get_engine_in_memory():
    # use sqlite in-memory
    engine = get_engine("sqlite:///:memory:")
    assert isinstance(engine, db.engine)


def test_create_tables_no_error():
    engine = get_engine("sqlite:///:memory:")
    create_sp500_companies_table(engine)
    create_sp500_changes_table(engine)
    create_price_table(engine)
    # If no exception raised, assume success
    assert True
