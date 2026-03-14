source("r/sparklabs/batch/sales/dataframe/utils.R")

joined <- join(orders, customers, orders$customer_id == customers$customer_id)
result <- orderBy(
  agg(groupBy(joined, joined$city), total_orders_per_city = count(joined$customer_id)),
  desc("total_orders_per_city")
)
showDF(result, truncate = FALSE)
