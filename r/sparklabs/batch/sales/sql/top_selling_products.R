source("r/sparklabs/batch/sales/sql/utils.R")

showDF(run_sql("
  SELECT oi.product_id, COUNT(*) AS total_orders
  FROM order_items oi
  JOIN products p ON oi.product_id = p.product_id
  GROUP BY oi.product_id
  ORDER BY total_orders DESC
"), truncate = FALSE)
