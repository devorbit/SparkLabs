source("r/sparklabs/batch/sales/sql/utils.R")

customer_count <- collect(run_sql("
  SELECT COUNT(customer_id) AS total_customers
  FROM orders
"))$total_customers[[1]]

cat(sprintf("Total distinct customers: %s\n", customer_count))
cat("Most Expensive Product:\n")
showDF(run_sql("
  SELECT *
  FROM products
  ORDER BY price DESC
  LIMIT 1
"), truncate = FALSE)
