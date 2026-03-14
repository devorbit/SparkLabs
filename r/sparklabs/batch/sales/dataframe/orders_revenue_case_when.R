source("r/sparklabs/batch/sales/dataframe/utils.R")

base_df <- join(order_items, orders, order_items$order_id == orders$order_id)
base_df <- join(base_df, products, base_df$product_id == products$product_id)
base_df <- join(base_df, promotions, base_df$category == promotions$category, "left")
base_df <- withColumn(base_df, "gross_revenue", base_df$price * base_df$quantity)
base_df <- withColumn(
  base_df,
  "discount_pct_active",
  ifelse(
    to_date(base_df$order_date) >= to_date(base_df$start_date) & to_date(base_df$order_date) <= to_date(base_df$end_date),
    coalesce(base_df$discount_pct, lit(0)),
    lit(0)
  )
)
base_df <- withColumn(base_df, "discount_value", base_df$gross_revenue * base_df$discount_pct_active / 100)
base_df <- withColumn(base_df, "net_revenue", base_df$gross_revenue - base_df$discount_value)

result <- orderBy(
  agg(
    groupBy(base_df, base_df$order_id),
    gross_revenue = round(sum(base_df$gross_revenue), 2),
    discount_value = round(sum(base_df$discount_value), 2),
    net_revenue = round(sum(base_df$net_revenue), 2)
  ),
  desc("net_revenue")
)
showDF(result, truncate = FALSE)
