source("r/sparklabs/batch/sales/sql/utils.R")

showDF(run_sql("
  SELECT c.*
  FROM customers c
  LEFT ANTI JOIN orders o ON c.customer_id = o.customer_id
"), truncate = FALSE)
