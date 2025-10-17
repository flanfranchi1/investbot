import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium", auto_download=["html"])


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from database import get_engine
    from config import POSTGRES_URL
    from transformations import stock_prices_transformations, catch_missing_prices, creating_sp500_index_timeline, sp500_changes_transformations, sp500_companies_transformations
    from utils import date_range
    from data_sourcing import get_market_working_days
    return (
        POSTGRES_URL,
        catch_missing_prices,
        creating_sp500_index_timeline,
        date_range,
        get_engine,
        get_market_working_days,
        mo,
        pl,
        sp500_changes_transformations,
        sp500_companies_transformations,
        stock_prices_transformations,
    )


@app.cell
def _(
    POSTGRES_URL,
    date_range,
    get_engine,
    get_market_working_days,
    pl,
    stock_prices_transformations,
):
    start_date, end_date = date_range(60)
    engine = get_engine(POSTGRES_URL)
    prices_df = stock_prices_transformations(engine=engine)
    pd_target_dates = get_market_working_days(end_date=end_date, start_date=start_date)
    target_dates_df = pl.from_pandas(pd_target_dates)
    return engine, prices_df, target_dates_df


@app.cell
def _(mo, target_dates_df):
    mo.ui.dataframe(target_dates_df.describe())
    return


@app.cell
def _(
    catch_missing_prices,
    creating_sp500_index_timeline,
    engine,
    prices_df,
    sp500_changes_transformations,
    sp500_companies_transformations,
    target_dates_df,
):
    companies_df = sp500_companies_transformations(engine=engine)
    changes_df = sp500_changes_transformations(engine=engine)
    timeline_df = creating_sp500_index_timeline(changes_df, companies_df, target_dates_df)
    missing_ranges = catch_missing_prices(prices_df, timeline_df)
    return (missing_ranges,)


@app.cell
def _(missing_ranges, mo):
    mo.ui.dataframe(missing_ranges.sort('ticker'))
    return


if __name__ == "__main__":
    app.run()
