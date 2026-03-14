package io.devorbit.sparklabs

import org.apache.spark.sql.functions._

object TopSellingProducts extends App {

  Utils.orderItems.join(Utils.products, Seq("product_id"), "inner")
    .groupBy(col("product_id"))
    .agg(count("*").as("total_orders"))
    .orderBy(desc("total_orders"))
    .show(false)

  Thread.sleep(1000000)
}
