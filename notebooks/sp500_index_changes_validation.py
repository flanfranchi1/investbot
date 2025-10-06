import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium", auto_download=["html"])


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from sqlalchemy import create_engine
    import pandas_market_calendars as mcal
    db_uri = "postgresql+psycopg2://investbot_user:investbot_password@localhost/investbot_db"
        
    return create_engine, db_uri, mo, pl


@app.cell
def _(create_engine, db_uri, mo, pl):
    engine = create_engine(db_uri)
    changes = (pl.read_database(connection=engine, query="""
        SELECT effective_date, added_ticker, removed_ticker FROM sp500_changes;
                               """)
        .with_columns(pl.col("effective_date").str.strptime(pl.Date, format="%B %e, %Y", strict=True).alias("date"))
        .drop("effective_date")
        .unpivot(index="date", on=["added_ticker", "removed_ticker"])
        .select(
            pl.col('date'),
            pl.col('variable').str.strip_suffix('_ticker').alias("action"),
            pl.col('value').alias('ticker')
        )
        .filter(pl.col('ticker').str.len_chars() >= 1)
        .sort(pl.col('ticker', 'date'))
    )
    mo.ui.table(changes)
    return changes, engine


@app.cell
def _(changes, mo, pl):
    previous_value = pl.col('action').shift(-1).over('ticker')
    validate_order = pl.when(
        pl.col('action') != previous_value
    ).then(
        pl.lit(True)
    ).when(
        previous_value.is_null()
    ).then(
        pl.lit(True)
    ).otherwise(
        pl.lit(False)
    ).alias('validate')

    validation_df = (
    
        changes
        .unique(pl.col('ticker', 'date', 'action'), keep='first')
        .with_columns(validate_order)
    )
    mo.ui.table(
        validation_df
        .group_by('validate')
        .len()
    )
    return (validation_df,)


@app.cell
def _(mo, pl, validation_df):
    not_validated = validation_df.filter(pl.col('validate') == False).select(pl.col('ticker'))
    tickers_for_analysis =(
        validation_df
        .join(not_validated, on='ticker', how='inner')
      .sort(pl.col('ticker', 'date'))  
    )

    mo.ui.table(tickers_for_analysis)
    return (tickers_for_analysis,)


@app.cell
def _(engine, mo, pl, tickers_for_analysis):
    tickers_list = "\'" + r"', '".join(
        list(
            set(
                tickers_for_analysis['ticker'].to_list()
            )
        )
    ) + "\'"

    sql_query = f"""
    SELECT *, (CASE WHEN (added_ticker in ({tickers_list}) OR removed_ticker IN ({tickers_list})) THEN 1 END) AS TARGET_TICKER FROM sp500_changes
        ;"""
    raw_changes = pl.read_database(query=sql_query, connection=engine)
    mo.ui.table(raw_changes)
    return


if __name__ == "__main__":
    app.run()
