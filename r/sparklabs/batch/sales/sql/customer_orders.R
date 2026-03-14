source("r/sparklabs/batch/sales/sql/utils.R")

showDF(run_sql("
  SELECT c.customer_id, approx_count_distinct(c.customer_id) AS total_orders
  FROM customers c
  JOIN orders o ON c.customer_id = o.customer_id
  GROUP BY c.customer_id
  ORDER BY total_orders DESC
"), truncate = FALSE)
