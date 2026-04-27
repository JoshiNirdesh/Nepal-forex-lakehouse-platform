SELECT 
    rate_date,
    buy_rate,
    sell_rate
FROM nepal_forex_db.fact_nrb_forex
WHERE currency_code = 'USD'
ORDER BY rate_date;