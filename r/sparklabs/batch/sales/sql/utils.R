source("r/sparklabs/batch/sales/dataframe/utils.R")
register_temp_views()

run_sql <- function(query) {
  sql(query)
}
