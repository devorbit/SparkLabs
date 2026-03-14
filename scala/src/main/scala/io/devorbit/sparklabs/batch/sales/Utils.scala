package io.devorbit.sparklabs.batch.sales

import java.nio.file.{Files, Paths}
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

  private val salesDataRoot = {
    val cwd = Paths.get("").toAbsolutePath.normalize()
    val candidates = Seq(
      cwd.resolve("data").resolve("sales"),
      cwd.resolve("..").resolve("data").resolve("sales")
    ).map(_.normalize())

    candidates.find(Files.exists(_)).getOrElse {
      throw new IllegalStateException(
        s"Unable to locate shared sales data. Checked: ${candidates.mkString(", ")}"
      )
    }
  }
  private def salesPath(fileName: String): String = salesDataRoot.resolve(fileName).toString

  private val customerPath = salesPath("customers.csv")
  private val deliveriesPath = salesPath("deliveries.csv")
  private val eventStreamPath = salesPath("events_stream.csv")
  private val inventoryPath = salesPath("inventory.csv")
  private val orderItemsPath = salesPath("order_items.csv")
  private val ordersPath = salesPath("orders.csv")
  private val productsPath = salesPath("products.csv")
  private val promotionsPath = salesPath("promotions.csv")
  private val returnsPath = salesPath("returns.csv")
  private val salesRepPath = salesPath("sales_reps.csv")
  private val storesPath = salesPath("stores.csv")

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
