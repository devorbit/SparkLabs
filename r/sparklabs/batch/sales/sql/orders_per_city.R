source("r/sparklabs/batch/sales/sql/utils.R")

showDF(run_sql("
  SELECT c.city, COUNT(*) AS total_orders_per_city
  FROM orders o
  JOIN customers c ON o.customer_id = c.customer_id
  GROUP BY c.city
  ORDER BY total_orders_per_city DESC
"), truncate = FALSE)
