source("r/sparklabs/batch/sales/dataframe/utils.R")

result <- orderBy(
  agg(groupBy(products, products$category), avg_price = round(avg(products$price))),
  desc("avg_price")
)
showDF(result, truncate = FALSE)
