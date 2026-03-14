source("r/sparklabs/batch/sales/dataframe/utils.R")

result <- orderBy(
  agg(groupBy(order_items, order_items$product_id), total_orders = count(order_items$product_id)),
  desc("total_orders")
)
showDF(result, truncate = FALSE)
