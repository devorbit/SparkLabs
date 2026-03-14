source("r/sparklabs/batch/sales/sql/utils.R")

showDF(run_sql("
  SELECT oi.order_id,
         SUM(p.price * oi.quantity) AS total_revenue,
         COUNT(*) AS total_orders
  FROM order_items oi
  JOIN products p ON oi.product_id = p.product_id
  GROUP BY oi.order_id
  ORDER BY total_revenue DESC
"), truncate = FALSE)
