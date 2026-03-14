package io.devorbit.sparklabs.batch.sales.dataset

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, dense_rank, desc}

object ProductRank extends App {
  val windowSpec = Window.orderBy(desc("category_count"))

  DatasetUtils.toDataFrame(DatasetUtils.orderItems)
    .groupBy(col("product_id"))
    .agg(count("*").as("order_count"))
    .join(DatasetUtils.toDataFrame(DatasetUtils.products), Seq("product_id"), "inner")
    .groupBy(col("category"))
    .agg(count("*").as("category_count"))
    .withColumn("category_rank", dense_rank().over(windowSpec))
    .show(false)
}
