SELECT
    missing_dates_status,
    COUNT(*) AS count
FROM
    MISSING_DATES_RANGE
GROUP BY
    missing_dates_status;