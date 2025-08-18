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
    from sourcing import get_sp500_comppanies_data
    from main import SP_500_URL
    COMPANIES = get_sp500_comppanies_data(SP_500_URL)
    print(COMPANIES)
    return (COMPANIES,)


@app.cell
def _(COMPANIES):
    from database import get_engine
    engine = get_engine()
    COMPANIES.to_sql(name='sp500_companies', con=engine,if_exists='replace')
    return


if __name__ == "__main__":
    app.run()
