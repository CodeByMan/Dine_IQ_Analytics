SELECT Location_ID,
       SUM(COALESCE(`order_items__Line_Total`, 0)) AS revenue,
       COUNT(DISTINCT Order_ID) AS order_count,
       SUM(COALESCE(`order_items__Quantity`, 0)) AS units_sold
FROM {{view_name}}
GROUP BY Location_ID
ORDER BY Location_ID
