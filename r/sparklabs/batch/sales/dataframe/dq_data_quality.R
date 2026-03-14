source("r/sparklabs/batch/sales/dataframe/utils.R")

duplicates <- where(
  count(groupBy(customers, customers$customer_id)),
  "count > 1"
)

if (count(duplicates) == 0) {
  cat("PASS: customer_id is unique\n")
} else {
  cat("FAIL: duplicate customer_id values found\n")
  showDF(duplicates, truncate = FALSE)
}
