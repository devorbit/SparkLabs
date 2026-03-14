source("r/sparklabs/batch/sales/sql/utils.R")

showDF(run_sql("
  SELECT id, CAST(rand() * 10 AS INT) AS salt
  FROM RANGE(20)
"), truncate = FALSE)
