package io.devorbit.sparklabs.batch.sales

import io.devorbit.sparklabs.batch.sales.Utils
import org.apache.spark.sql.functions._

object RevenuePerStore extends App {

  private val revenuePerOrder = Utils.orderItems.join(Utils.products, Seq("product_id"), "inner")
    .withColumn("revenue", col("price") * col("quantity"))
    .groupBy(col("order_id"))
    .agg(sum("revenue").as("total_revenue"), count("*").as("total_orders"))
    .orderBy(desc("total_revenue"))

  Utils.orders
    .join(revenuePerOrder, Seq("order_id"), "inner")
    .join(Utils.stores, Seq("store_id"), "inner")
    .groupBy(col("store_id"))
    .agg(sum("total_revenue").as("store_revenue"), count("*").as("total_store_orders"))
    .orderBy(desc("store_revenue"))
    .show(false)

//  Thread.sleep(1000000)
}
