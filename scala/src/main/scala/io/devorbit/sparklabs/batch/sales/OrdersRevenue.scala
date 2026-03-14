package io.devorbit.sparklabs.batch.sales

import io.devorbit.sparklabs.batch.sales.Utils
import org.apache.spark.sql.functions._

object OrdersRevenue extends App {

  Utils.orderItems.join(Utils.products, Seq("product_id"), "inner")
    .withColumn("revenue", col("price") * col("quantity"))
    .groupBy(col("order_id"))
    .agg(sum("revenue").as("total_revenue"), count("*").as("total_orders"))
    .orderBy(desc("total_revenue"))
    .show(false)

//  Thread.sleep(1000000)
}
