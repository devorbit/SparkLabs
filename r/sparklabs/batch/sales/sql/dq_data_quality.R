source("r/sparklabs/batch/sales/sql/utils.R")

duplicates <- run_sql("
  SELECT customer_id, COUNT(*) AS duplicate_count
  FROM customers
  GROUP BY customer_id
  HAVING COUNT(*) > 1
")

if (count(duplicates) == 0) {
  cat("PASS: customer_id is unique\n")
} else {
  cat("FAIL: duplicate customer_id values found\n")
  showDF(duplicates, truncate = FALSE)
}
