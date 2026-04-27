SELECT 
    rate_date,
    currency_code,
    sell_rate,
    daily_change_percent
FROM gold_forex_daily_change
WHERE ABS(daily_change_percent) >= 1
ORDER BY ABS(daily_change_percent) DESC;