CREATE TEMP VIEW IF NOT EXISTS overall_dates AS
SELECT
    MIN(date) AS start_date,
    MAX(date) AS end_date
FROM
    stock_prices;

CREATE TEMP VIEW IF NOT EXISTS stock_dates AS
SELECT
    ticker,
    MIN(date) AS stock_start_date,
    MAX(date) AS stock_end_date
FROM
    stock_prices
GROUP BY
    ticker;

CREATE TABLE
    MISSING_DATES_RANGE AS
SELECT
    stock_dates.ticker,
    overall_dates.start_date,
    overall_dates.end_date,
    stock_dates.stock_start_date,
    stock_dates.stock_end_date,
    CASE
        WHEN stock_dates.stock_start_date > overall_dates.start_date
        AND stock_dates.stock_end_date < overall_dates.end_date THEN 'Missing start and end dates'
        WHEN stock_dates.stock_start_date > overall_dates.start_date THEN 'Missing start dates'
        WHEN stock_dates.stock_end_date < overall_dates.end_date THEN 'Missing end dates'
        ELSE 'No missing dates'
    END AS missing_dates_status
FROM
    stock_dates
    CROSS JOIN overall_dates;