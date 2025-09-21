# -*- coding: utf-8 -*-

import config
import polars as pl
from database import get_engine


def sp500_companies_transformations() -> pl.DataFrame:
    """Applies transformations to the sp500_companies table data."""
    engine = get_engine(config.SQLITE_DB_PATH)
    df = pl.read_database("SELECT * FROM sp500_companies", engine)
    df = df.select(
        pl.col('symbol').str.strip_chars().alias('ticker'),
        pl.col('security').str.strip_chars().alias('company_name'),
        pl.col('gics_sector').str.strip_chars().alias('sector'),
        pl.col('gics_sub_industry').str.strip_chars().alias('sub_industry'),
        pl.col('headquarters_location').str.strip_chars().alias('headquarters'),
        pl.col('date_added').str.strip_chars().alias('date_added'),
        pl.col('cik').str.strip_chars().alias('cik'),
        pl.col('founded').cast(pl.Int8, strict=False).alias('founded_year'),
        # pl.col('registry_date').str.strptime(pl.Date,format='%Y-%m-%d', strict=False).alias('registry_date')
    )
    df = df.drop_nulls(subset=['ticker'])
    df = df.unique(subset=['ticker', 'date_added'])
    return df


def sp500_changes_transformations() -> pl.DataFrame:
    """Applies transformations to the sp500_changes table data."""
    engine = get_engine(config.SQLITE_DB_PATH)
    df = pl.read_database("SELECT * FROM sp500_changes", engine)
    df = df.select([
        (pl
         .col('effective_date')
         .str.strip_chars()
         .str.strptime(pl.Date, format='%B %e, %Y', strict=False)
         .alias('effective_date')),
        pl.col('added_ticker').str.strip_chars().alias('added_ticker'),
        pl.col('added_security').str.strip_chars().alias('added_security'),
        pl.col('removed_ticker').str.strip_chars().alias('removed_ticker'),
        pl.col('removed_security').str.strip_chars().alias('removed_security'),
        # pl.col('data_added').str.strptime(pl.Date, format='%Y-%m-%d', strict=False).alias('data_added'),
        pl.col('reason').str.strip_chars().alias('reason')
    ])
    df = df.drop_nulls(subset=['effective_date'])
    df = df.unique(subset=['effective_date', 'added_ticker', 'removed_ticker'])
    return df


def stock_prices_transformations() -> pl.DataFrame:
    """Applies transformations to the stock_prices table data."""
    engine = get_engine(config.SQLITE_DB_PATH)
    df = pl.read_database("SELECT * FROM stock_prices", engine)
    df = df.select([
        pl.col('ticker').str.strip_chars().alias('ticker')
        #     pl.col('date').str.strptime(pl.Date, fmt='%Y-%m-%d', strict=False).alias('date'),
        #     pl.col('open').cast(pl.Float64).alias('open'),
        #     pl.col('high').cast(pl.Float64).alias('high'),
        #     pl.col('low').cast(pl.Float64).alias('low'),
        #     pl.col('close').cast(pl.Float64).alias('close'),
        #     pl.col('adj_close').cast(pl.Float64).alias('adj_close'),
        #     pl.col('volume').cast(pl.Int64).alias('volume')
    ])
    df = df.drop_nulls(subset=['ticker', 'date'])
    df = df.unique(subset=['ticker', 'date'])
    return df


def creating_sp500_index_timeline(
    changes_df: pl.DataFrame,
    companies_df: pl.DataFrame,
    trading_days: pl.Series
) -> pl.DataFrame:
    """Creates a timeline of S&P 500 index changes."""
    tickers = (
        changes_df
        .select(pl.col(
            'added_ticker', 'removed_ticker', 'effective_date'
        ))
        .unpivot(index='effective_date', on=['added_ticker', 'removed_ticker'])
        .select(pl.col('value').alias('ticker'))
        .vstack((companies_df
                 .select(pl.col('ticker')
                         )))
        .drop_nulls()
        .unique()
        ['ticker']
        .to_frame()
        .with_columns(pl.lit(0).alias('join_key'))
    )

    dates = (
        trading_days
        .to_frame()
        .with_columns(pl.lit(0).alias('join_key'))
    )

    tickers_and_dates_df = (
        dates
        .join(tickers, on='join_key', how='inner')
        .select(
            pl.col('').cast(pl.Date).alias('date'),
            pl.col('ticker')
        )
    )

    pivoted_date_ranges = (
        changes_df
        .unpivot(index='effective_date', on=['added_ticker', 'removed_ticker'])
        .sort(['value', 'effective_date'])
        .select(
            pl.col('effective_date').alias('date'),
            pl.col('value').alias('ticker'),
            pl.col('variable').count().over(
                pl.col('value')).alias('action_count'),
            pl.when(pl.col('variable') == 'added_ticker').then(pl.lit('added')).otherwise(
                pl.when(pl.col('variable') == 'removed_ticker').then(
                    pl.lit('removed')).otherwise(pl.lit('unknown'))
            ).alias('action_type')
        )
    )

    missing_added_dates = (
        pivoted_date_ranges
        .filter((pl.col('action_type') == 'removed') & (pl.col('action_count') == 1))
        .with_columns(pl.lit('added').alias('action_type'),
                      pl.lit(dates[''].min().date()).alias('date'))
    )

    final_data = (pivoted_date_ranges.vstack(missing_added_dates)
                  .sort(['ticker', 'date'])
                  .select(
        pl.col('ticker'),
        pl.when(pl.col('action_type') == 'added').then(
            pl.col('date')).otherwise(pl.lit(None)).alias('added_date'),
        pl.col('date').shift(-1).fill_null(pl.lit(dates[''].max().date())
                                           ).alias('removed_date')
    )
        .join(tickers_and_dates_df, on='ticker', how='inner')
        .filter(
            (pl.col('date') >= pl.col('added_date')) &
            (pl.col('date') < pl.col('removed_date')) &
            (pl.col('ticker').str.len_chars() >=1)
        )
    )

    return final_data


if __name__ == "__main__":
    import config
    from data_sourcing import get_market_working_days
    engine = get_engine(config.SQLITE_DB_PATH)
    companies_df = sp500_companies_transformations()
    changes_df = sp500_changes_transformations()
    trading_days = pl.from_pandas(
        get_market_working_days("2023-01-01", "2024-12-31"))
    timeline_df = creating_sp500_index_timeline(
        changes_df,
        companies_df,
        trading_days
    )
    print(timeline_df)
