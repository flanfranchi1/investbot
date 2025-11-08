import polars as pl
import datetime as dt
from transformations import (
    creating_sp500_index_timeline,
    catch_missing_prices,
)


def test_creating_sp500_index_timeline_basic():
    # prepare changes df
    changes = pl.DataFrame({
        "effective_date": [dt.date(2020,1,2), dt.date(2020,2,1)],
        "added_ticker": ["A", None],
        "removed_ticker": [None, "B"]
    })
    companies = pl.DataFrame({"ticker": ["A", "B"]})
    trading_days = pl.Series([dt.date(2020,1,1), dt.date(2020,1,2), dt.date(2020,1,3), dt.date(2020,2,1)])
    timeline = creating_sp500_index_timeline(changes, companies, trading_days)
    assert "ticker" in timeline.columns
    # timeline may be empty depending on filtering; ensure expected columns exist
    assert set(["date", "ticker", "added_date", "removed_date"]).issubset(set(timeline.columns))


def test_catch_missing_prices_empty_prices():
    prices = pl.DataFrame()
    timeline = pl.DataFrame({"ticker": ["A","A"], "date": [dt.date(2020,1,1), dt.date(2020,1,2)]})
    grouped = catch_missing_prices(prices, timeline)
    assert isinstance(grouped, pl.DataFrame)


def test_catch_missing_prices_with_prices():
    prices = pl.DataFrame({"ticker":["A","A","A"], "date":[dt.date(2020,1,1), dt.date(2020,1,2), dt.date(2020,1,3)], "open":[10.0, None, None]})
    timeline = pl.DataFrame({"ticker":["A","A","A"], "date":[dt.date(2020,1,1), dt.date(2020,1,2), dt.date(2020,1,3)]})
    grouped = catch_missing_prices(prices, timeline)
    # grouped should be a DataFrame with specific columns even if empty
    assert set(["ticker", "first_missing_date", "last_missing_date"]).issubset(set(grouped.columns))
