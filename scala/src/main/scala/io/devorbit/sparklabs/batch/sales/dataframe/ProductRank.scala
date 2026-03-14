package io.devorbit.sparklabs.batch.sales.dataframe

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ProductRank extends App {

  val windowSpec = Window
    .orderBy(desc("category_count"))

  Utils.orderItems
    .groupBy(col("product_id"))
    .agg(count("*").as("order_count"))
    .join(Utils.products, Seq("product_id"), "inner")
    .groupBy(col("category"))
    .agg(count("*").as("category_count"))
    .withColumn("category_rank", dense_rank().over(windowSpec))
    .show(false)

//  Thread.sleep(1000000)
}
