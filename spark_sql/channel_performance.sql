SELECT Channel,
       SUM(COALESCE(`order_items__Line_Total`, 0)) AS revenue,
       COUNT(DISTINCT Order_ID) AS order_count
FROM {{view_name}}
GROUP BY Channel
ORDER BY Channel
