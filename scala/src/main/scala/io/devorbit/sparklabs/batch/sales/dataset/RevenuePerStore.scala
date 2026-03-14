package io.devorbit.sparklabs.batch.sales.dataset

import org.apache.spark.sql.functions.{col, count, desc, sum}

object RevenuePerStore extends App {
  private val revenuePerOrder = DatasetUtils.toDataFrame(DatasetUtils.orderItems)
    .join(DatasetUtils.toDataFrame(DatasetUtils.products), Seq("product_id"), "inner")
    .withColumn("revenue", col("price") * col("quantity"))
    .groupBy(col("order_id"))
    .agg(sum("revenue").as("total_revenue"), count("*").as("total_orders"))
    .orderBy(desc("total_revenue"))

  DatasetUtils.toDataFrame(DatasetUtils.orders)
    .join(revenuePerOrder, Seq("order_id"), "inner")
    .join(DatasetUtils.toDataFrame(DatasetUtils.stores), Seq("store_id"), "inner")
    .groupBy(col("store_id"))
    .agg(sum("total_revenue").as("store_revenue"), count("*").as("total_store_orders"))
    .orderBy(desc("store_revenue"))
    .show(false)
}
