spark_home <- Sys.getenv("SPARK_HOME", unset = "")

if (spark_home == "") {
  stop("SPARK_HOME is not set. Point it to your Apache Spark installation before running SparkLabs R scripts.")
}

spark_r_lib <- file.path(spark_home, "R", "lib")
spark_r_pkg <- file.path(spark_r_lib, "SparkR")

if (!dir.exists(spark_r_pkg)) {
  stop(sprintf("SparkR was not found under SPARK_HOME. Expected: %s", spark_r_pkg))
}

.libPaths(c(normalizePath(spark_r_lib, winslash = "/", mustWork = TRUE), .libPaths()))
suppressWarnings(library(SparkR))

project_root <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)
sales_data_root <- normalizePath(file.path(project_root, "data", "sales"), winslash = "/", mustWork = TRUE)

if (!exists("spark")) {
  spark <- sparkR.session(master = "local[*]", appName = "SparkLabsR")
}

sales_path <- function(file_name) {
  file.path(sales_data_root, file_name)
}

read_sales_csv <- function(file_name) {
  read.df(
    sales_path(file_name),
    source = "csv",
    header = "true",
    inferSchema = "true"
  )
}

customers <- read_sales_csv("customers.csv")
deliveries <- read_sales_csv("deliveries.csv")
events <- read_sales_csv("events_stream.csv")
inventory <- read_sales_csv("inventory.csv")
order_items <- read_sales_csv("order_items.csv")
orders <- read_sales_csv("orders.csv")
products <- read_sales_csv("products.csv")
promotions <- read_sales_csv("promotions.csv")
returns <- read_sales_csv("returns.csv")
sales_reps <- read_sales_csv("sales_reps.csv")
stores <- read_sales_csv("stores.csv")

register_temp_views <- function() {
  createOrReplaceTempView(customers, "customers")
  createOrReplaceTempView(deliveries, "deliveries")
  createOrReplaceTempView(events, "events")
  createOrReplaceTempView(inventory, "inventory")
  createOrReplaceTempView(order_items, "order_items")
  createOrReplaceTempView(orders, "orders")
  createOrReplaceTempView(products, "products")
  createOrReplaceTempView(promotions, "promotions")
  createOrReplaceTempView(returns, "returns")
  createOrReplaceTempView(sales_reps, "sales_reps")
  createOrReplaceTempView(stores, "stores")
}
