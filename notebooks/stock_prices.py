import marimo

__generated_with = "0.14.17"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import sys
    import os

    project_root = os.path.abspath(os.getcwd())
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    return


@app.cell
def _():
    from database import get_engine
    import config
    import pandas as pd

    engine = get_engine(config.SQLITE_DB_PATH)
    stock_prices_df = pd.read_sql(
        sql="SELECT * FROM STOCK_PRICES;",
        con=engine,
        parse_dates=['date']
    )

    print(stock_prices_df)

    return (stock_prices_df,)


@app.cell
def _():
    return


@app.cell
def _(stock_prices_df):
    print(
        stock_prices_df
        ['ticker']
        .value_counts()
    )
    return


if __name__ == "__main__":
    app.run()
