source("r/sparklabs/batch/sales/dataframe/utils.R")

revenue_df <- withColumn(
  join(order_items, products, order_items$product_id == products$product_id),
  "revenue",
  products$price * order_items$quantity
)
revenue_per_order <- agg(
  groupBy(revenue_df, revenue_df$order_id),
  total_revenue = sum(revenue_df$revenue),
  total_orders = count(revenue_df$order_id)
)
joined <- join(orders, revenue_per_order, orders$order_id == revenue_per_order$order_id)
joined <- join(joined, stores, joined$store_id == stores$store_id)
result <- orderBy(
  agg(groupBy(joined, joined$store_id), store_revenue = sum(joined$total_revenue), total_store_orders = count(joined$store_id)),
  desc("store_revenue")
)
showDF(result, truncate = FALSE)
