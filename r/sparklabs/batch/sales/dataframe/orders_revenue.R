source("r/sparklabs/batch/sales/dataframe/utils.R")

revenue_df <- withColumn(
  join(order_items, products, order_items$product_id == products$product_id),
  "revenue",
  products$price * order_items$quantity
)
result <- orderBy(
  agg(groupBy(revenue_df, revenue_df$order_id), total_revenue = sum(revenue_df$revenue), total_orders = count(revenue_df$order_id)),
  desc("total_revenue")
)
showDF(result, truncate = FALSE)
