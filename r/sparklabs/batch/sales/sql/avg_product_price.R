source("r/sparklabs/batch/sales/sql/utils.R")

showDF(run_sql("
  SELECT category, ROUND(AVG(price)) AS avg_price
  FROM products
  GROUP BY category
  ORDER BY avg_price DESC
"), truncate = FALSE)
