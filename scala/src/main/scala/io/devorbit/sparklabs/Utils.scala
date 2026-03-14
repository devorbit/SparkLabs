package io.devorbit.sparklabs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, desc}

object Utils {

  val sparkSession = SparkSession
    .builder()
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .appName("Runner2")
    .master("local[*]")
    .getOrCreate()

  private val reader = sparkSession
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")

  private val customerPath = "/Users/jayantkumar/Workspace/Code/SparkLabs/scala/data/sales/customers.csv"
  private val deliveriesPath = "/Users/jayantkumar/Workspace/Code/SparkLabs/scala/data/sales/deliveries.csv"
  private val eventStreamPath = "/Users/jayantkumar/Workspace/Code/SparkLabs/scala/data/sales/events_stream.csv"
  private val inventoryPath = "/Users/jayantkumar/Workspace/Code/SparkLabs/scala/data/sales/inventory.csv"
  private val orderItemsPath = "/Users/jayantkumar/Workspace/Code/SparkLabs/scala/data/sales/order_items.csv"
  private val ordersPath = "/Users/jayantkumar/Workspace/Code/SparkLabs/scala/data/sales/orders.csv"
  private val productsPath = "/Users/jayantkumar/Workspace/Code/SparkLabs/scala/data/sales/products.csv"
  private val promotionsPath = "/Users/jayantkumar/Workspace/Code/SparkLabs/scala/data/sales/promotions.csv"
  private val returnsPath = "/Users/jayantkumar/Workspace/Code/SparkLabs/scala/data/sales/returns.csv"
  private val salesRepPath = "/Users/jayantkumar/Workspace/Code/SparkLabs/scala/data/sales/sales_reps.csv"
  private val storesPath = "/Users/jayantkumar/Workspace/Code/SparkLabs/scala/data/sales/stores.csv"

  lazy val customers = reader.load(customerPath)
  lazy val deliveries = reader.load(deliveriesPath)
  lazy val events = reader.load(eventStreamPath)
  lazy val inventory = reader.load(inventoryPath)
  lazy val orderItems = reader.load(orderItemsPath)
  lazy val orders = reader.load(ordersPath)
  lazy val products = reader.load(productsPath)
  lazy val promotions = reader.load(promotionsPath)
  lazy val returns = reader.load(returnsPath)
  lazy val salesRep = reader.load(salesRepPath)
  lazy val stores = reader.load(storesPath)

  println(s"Total order count: ${orders.count()}")
// orders count per store
 private lazy val storeOrders =  orders
    .groupBy(col("store_id"))
    .agg(count("order_id").as("order_count"))
    .where(col("order_count") > 250 and col("order_count") < 300)
    .orderBy(desc("order_count"))
    .selectExpr("store_id", "order_count")

  // storeOrders.join(stores, Seq("store_id"), "inner").show(false)

//  Thread.sleep(1000000)
}
