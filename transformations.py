# -*- coding: utf-8 -*-

import polars as pl
import sqlalchemy as db
from data_sourcing import get_market_working_days
from utils import pivoting_dict
import logging


def sp500_companies_transformations(engine: db.Engine) -> pl.DataFrame:
    """Applies transformations to the sp500_companies table data."""

    df = pl.read_database("SELECT * FROM sp500_companies", engine)
    if df.is_empty:
        df = df.rename(mapping={"symbol": "ticker"})
        logging.warning("sp500_companies table is empty.")
    else:
        df = df.select(
            pl.col("symbol").str.strip_chars().alias("ticker"),
            pl.col("security").str.strip_chars().alias("company_name"),
            pl.col("gics_sector").str.strip_chars().alias("sector"),
            pl.col("gics_sub_industry").str.strip_chars().alias("sub_industry"),
            pl.col("headquarters_location").str.strip_chars().alias("headquarters"),
            pl.col("date_added").str.strip_chars().alias("date_added"),
            pl.col("cik").str.strip_chars().alias("cik"),
            pl.col("founded").cast(pl.Int8, strict=False).alias("founded_year"),
        )
        df = df.drop_nulls(subset=["ticker"])
        df = df.unique(subset=["ticker", "date_added"])
    return df


def sp500_changes_transformations(engine: db.Engine) -> pl.DataFrame:
    """Applies transformations to the sp500_changes table data."""

    df = pl.read_database("SELECT * FROM sp500_changes", engine)
    df = df.select(
        [
            (
                pl.col("effective_date")
                .str.strip_chars()
                .str.strptime(pl.Date, format="%B %e, %Y", strict=False)
                .alias("effective_date")
            ),
            pl.col("added_ticker").str.strip_chars().alias("added_ticker"),
            pl.col("added_security").str.strip_chars().alias("added_security"),
            pl.col("removed_ticker").str.strip_chars().alias("removed_ticker"),
            pl.col("removed_security").str.strip_chars().alias("removed_security"),
            # pl.col('data_added').str.strptime(pl.Date, format='%Y-%m-%d', strict=False).alias('data_added'),
            pl.col("reason").str.strip_chars().alias("reason"),
        ]
    )
    df = df.drop_nulls(subset=["effective_date"])
    df = df.unique(subset=["effective_date", "added_ticker", "removed_ticker"])
    return df


def stock_prices_transformations(engine: db.Engine) -> pl.DataFrame:
    """Applies transformations to the stock_prices table data."""
    df = pl.read_database("SELECT * FROM stock_prices", engine)
    if df.is_empty:
        logging.warning("stock_prices table is empty.")
    else:
        df = df.select(
            pl.col("ticker").str.strip_chars().alias("ticker"),
            pl.col("date")
            .str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
            .alias("date"),
            pl.col("open").cast(pl.Float64).alias("open"),
            pl.col("high").cast(pl.Float64).alias("high"),
            pl.col("low").cast(pl.Float64).alias("low"),
            pl.col("close").cast(pl.Float64).alias("close"),
            pl.col("volume").cast(pl.Int64).alias("volume"),
        )
        df = df.drop_nulls(subset=["ticker", "date"])
        df = df.unique(subset=["ticker", "date"])
    return df


def creating_sp500_index_timeline(
    changes_df: pl.DataFrame, companies_df: pl.DataFrame, trading_days: pl.Series
) -> pl.DataFrame:
    """Creates a timeline of S&P 500 index changes."""
    min_date = trading_days.min().date()

    tickers = (
        changes_df.filter(pl.col("effective_date") >= pl.lit(min_date))
        .select(pl.col("added_ticker", "removed_ticker", "effective_date"))
        .unpivot(index="effective_date", on=["added_ticker", "removed_ticker"])
        .select(pl.col("value").alias("ticker"))
        .vstack((companies_df.select(pl.col("ticker"))))
        .drop_nulls()
        .unique()["ticker"]
        .to_frame()
        .with_columns(pl.lit(0).alias("join_key"))
    )

    dates = trading_days.to_frame().with_columns(pl.lit(0).alias("join_key"))

    tickers_and_dates_df = dates.join(tickers, on="join_key", how="inner").select(
        pl.col("").cast(pl.Date).alias("date"), pl.col("ticker")
    )

    pivoted_date_ranges = (
        changes_df.unpivot(
            index="effective_date", on=["added_ticker", "removed_ticker"]
        )
        .sort(["value", "effective_date"])
        .select(
            pl.col("effective_date").alias("date"),
            pl.col("value").alias("ticker"),
            pl.col("variable").count().over(pl.col("value")).alias("action_count"),
            pl.when(pl.col("variable") == "added_ticker")
            .then(pl.lit("added"))
            .otherwise(
                pl.when(pl.col("variable") == "removed_ticker")
                .then(pl.lit("removed"))
                .otherwise(pl.lit("unknown"))
            )
            .alias("action_type"),
        )
    )

    missing_added_dates = pivoted_date_ranges.filter(
        (pl.col("action_type") == "removed") & (pl.col("action_count") == 1)
    ).with_columns(
        pl.lit("added").alias("action_type"),
        pl.lit(dates[""].min().date()).alias("date"),
    )

    final_data = (
        pivoted_date_ranges.vstack(missing_added_dates)
        .sort(["ticker", "date"])
        .select(
            pl.col("ticker"),
            pl.when(pl.col("action_type") == "added")
            .then(pl.col("date"))
            .otherwise(pl.lit(None))
            .alias("added_date"),
            pl.col("date")
            .shift(-1)
            .fill_null(pl.lit(dates[""].max().date()))
            .alias("removed_date"),
        )
        .join(tickers_and_dates_df, on="ticker", how="inner")
        .filter(
            (pl.col("date") >= pl.col("added_date"))
            & (pl.col("date") < pl.col("removed_date"))
            & (pl.col("ticker").str.len_chars() >= 1)
        )
        .drop("date")
    )

    return final_data


def catch_missing_prices(
    prices_df: pl.DataFrame, timeline_df: pl.DataFrame
) -> pl.DataFrame:
    """Identifies missing price entries in the stock_prices table."""
    if prices_df.is_empty:
        grouped = timeline_df.group_by("ticker").agg(
            pl.min("added_date").alias("first_missing_date"),
            pl.max("removed_date").alias("last_missing_date"),
        )

    else:
        merged_df = timeline_df.join(
            prices_df,
            on=["ticker", "date"],
            how="left",
            suffix="_price",
        )
        missing_prices_df = (
            merged_df.filter(pl.col("open").is_null())
            .select(pl.col("ticker"), pl.col("date"))
            .sort(["ticker", "date"])
        )

        missing_prices_df = missing_prices_df.with_columns(
            (
                pl.col("date").cast(pl.Int64) - pl.col("date").cast(pl.Int64).shift(1)
            ).alias("date_diff"),
            (
                (pl.col("ticker") != pl.col("ticker").shift(1))
                | (
                    pl.col("date").cast(pl.Int64)
                    - pl.col("date").cast(pl.Int64).shift(1)
                    > 1
                )
            )
            .cum_sum()
            .alias("group_id"),
        )

        grouped = (
            missing_prices_df.group_by(["ticker", "group_id"])
            .agg(
                [
                    pl.col("date").min().alias("first_missing_date"),
                    pl.col("date").max().alias("last_missing_date"),
                    pl.len().alias("missing_count"),
                ]
            )
            .filter(pl.col("missing_count") > 4)
            .select(["ticker", "first_missing_date", "last_missing_date"])
        )
    return grouped


def get_missing_price_ranges(start_date: str, end_date: str, engine=db.Engine) -> dict:
    """Fetches missing price ranges from the database."""

    companies_df = sp500_companies_transformations(engine=engine)
    changes_df = sp500_changes_transformations(engine=engine)
    trading_days = pl.from_pandas(get_market_working_days(start_date, end_date))
    timeline_df = creating_sp500_index_timeline(changes_df, companies_df, trading_days)
    stock_prices = stock_prices_transformations(engine=engine)
    missing_ranges = (
        catch_missing_prices(stock_prices, timeline_df)
        .with_columns(
            pl.col("first_missing_date")
            .dt.strftime("%Y-%m-%d")
            .alias("first_missing_date"),
            pl.col("last_missing_date")
            .dt.strftime("%Y-%m-%d")
            .alias("last_missing_date"),
        )
        .to_dict(as_series=False)
    )
    return pivoting_dict(missing_ranges)
