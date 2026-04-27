SELECT 
    currency_code,
    sell_rate
FROM nepal_forex_db.fact_nrb_forex
WHERE rate_date = (
    SELECT MAX(rate_date) 
    FROM nepal_forex_db.fact_nrb_forex
)
ORDER BY sell_rate DESC;