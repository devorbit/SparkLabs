source("r/sparklabs/batch/sales/dataframe/utils.R")

customer_ids_with_orders <- distinct(select(orders, "customer_id"))
result <- except(customers, join(customers, customer_ids_with_orders, customers$customer_id == customer_ids_with_orders$customer_id, "inner"))
showDF(result, truncate = FALSE)
