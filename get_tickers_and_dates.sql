SELECT DISTINCT
    REPLACE(ticker, '.', '-') AS ticker
    FROM sp500_companies
    WHERE date_added IS NOT NULL;