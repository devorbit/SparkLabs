source("r/sparklabs/batch/sales/dataframe/utils.R")

distinct_customer_count <- count(select(orders, "customer_id"))
expensive_product <- limit(orderBy(products, desc("price")), 1)

cat(sprintf("Total distinct customers: %s\n", distinct_customer_count))
cat("Most Expensive Product:\n")
showDF(expensive_product, truncate = FALSE)
