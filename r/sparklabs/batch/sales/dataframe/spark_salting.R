source("r/sparklabs/batch/sales/dataframe/utils.R")

salted <- withColumn(createDataFrame(data.frame(id = 0:19)), "salt", round(rand() * 10))
showDF(salted, truncate = FALSE)
