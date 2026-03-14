source("r/sparklabs/batch/sales/dataframe/utils.R")

order_counts <- agg(groupBy(order_items, order_items$product_id), order_count = count(order_items$product_id))
category_counts <- agg(
  groupBy(join(order_counts, products, order_counts$product_id == products$product_id), products$category),
  category_count = count(products$category)
)
result <- withColumn(
  category_counts,
  "category_rank",
  dense_rank() %over% orderBy(windowPartitionBy(), desc("category_count"))
)
showDF(result, truncate = FALSE)
