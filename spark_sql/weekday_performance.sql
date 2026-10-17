SELECT date_format(Order_Date, 'EEEE') AS weekday,
       SUM(COALESCE(`order_items__Line_Total`, 0)) AS revenue,
       COUNT(DISTINCT Order_ID) AS order_count
FROM {{view_name}}
GROUP BY date_format(Order_Date, 'EEEE')
ORDER BY weekday
