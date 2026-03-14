package io.devorbit.sparklabs.batch.sales.rdd

import java.nio.file.{Files, Paths}
import io.devorbit.sparklabs.batch.sales.dataframe.Utils
import io.devorbit.sparklabs.batch.sales.dataset._
import org.apache.spark.rdd.RDD

object RddUtils {

  private val sc = Utils.sparkSession.sparkContext
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

  private def csvRows(path: String): RDD[Array[String]] = {
    val lines = sc.textFile(path)
    val header = lines.first()
    lines.filter(_ != header).map(_.split(",", -1).map(_.trim))
  }

  lazy val customers: RDD[Customer] =
    csvRows(salesPath("customers.csv")).map(r => Customer(r(0).toInt, r(1), r(2), r(3), r(4), r(5)))
  lazy val deliveries: RDD[Delivery] =
    csvRows(salesPath("deliveries.csv")).map(r => Delivery(r(0).toInt, r(1).toInt, r(2), r(3), r(4)))
  lazy val events: RDD[Event] =
    csvRows(salesPath("events_stream.csv")).map(r => Event(r(0).toInt, r(1).toInt, r(2), r(3), r(4)))
  lazy val inventory: RDD[Inventory] =
    csvRows(salesPath("inventory.csv")).map(r => Inventory(r(0).toInt, r(1).toInt, r(2).toInt, r(3).toInt, r(4)))
  lazy val orderItems: RDD[OrderItem] =
    csvRows(salesPath("order_items.csv")).map(r => OrderItem(r(0).toInt, r(1).toInt, r(2).toInt, r(3).toInt))
  lazy val orders: RDD[Order] =
    csvRows(salesPath("orders.csv")).map(r => Order(r(0).toInt, r(1).toInt, r(2).toInt, r(3).toInt, r(4)))
  lazy val products: RDD[Product] =
    csvRows(salesPath("products.csv")).map(r => Product(r(0).toInt, r(1), r(2), r(3).toDouble))
  lazy val promotions: RDD[Promotion] =
    csvRows(salesPath("promotions.csv")).map(r => Promotion(r(0).toInt, r(1), r(2).toDouble, r(3), r(4)))
  lazy val returns: RDD[Return] =
    csvRows(salesPath("returns.csv")).map(r => Return(r(0).toInt, r(1).toInt, r(2), r(3)))
  lazy val salesReps: RDD[SalesRep] =
    csvRows(salesPath("sales_reps.csv")).map(r => SalesRep(r(0).toInt, r(1), r(2)))
  lazy val stores: RDD[Store] =
    csvRows(salesPath("stores.csv")).map(r => Store(r(0).toInt, r(1), r(2)))
}
