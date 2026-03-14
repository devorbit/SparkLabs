source("r/sparklabs/batch/sales/dataframe/utils.R")

result <- orderBy(
  agg(
    groupBy(join(customers, orders, customers$customer_id == orders$customer_id), customers$customer_id),
    total_orders = approx_count_distinct(customers$customer_id)
  ),
  desc("total_orders")
)
showDF(result, truncate = FALSE)
