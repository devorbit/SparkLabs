package io.devorbit.sparklabs.batch.sales.dataset

import io.devorbit.sparklabs.batch.sales.dataframe.Utils
import org.apache.spark.sql.{DataFrame, Dataset}

object DatasetUtils {
  import Utils.sparkSession.implicits._

  lazy val customers: Dataset[Customer] = Utils.customers.as[Customer]
  lazy val deliveries: Dataset[Delivery] = Utils.deliveries.as[Delivery]
  lazy val events: Dataset[Event] = Utils.events.as[Event]
  lazy val inventory: Dataset[Inventory] = Utils.inventory.as[Inventory]
  lazy val orderItems: Dataset[OrderItem] = Utils.orderItems.as[OrderItem]
  lazy val orders: Dataset[Order] = Utils.orders.as[Order]
  lazy val products: Dataset[Product] = Utils.products.as[Product]
  lazy val promotions: Dataset[Promotion] = Utils.promotions.as[Promotion]
  lazy val returns: Dataset[Return] = Utils.returns.as[Return]
  lazy val salesRep: Dataset[SalesRep] = Utils.salesRep.as[SalesRep]
  lazy val stores: Dataset[Store] = Utils.stores.as[Store]

  def toDataFrame[T](dataset: Dataset[T]): DataFrame = dataset.toDF()
}
