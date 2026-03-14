package io.devorbit.sparklabs.batch.sales.dataset

import org.apache.spark.sql.functions.{col, count, desc, sum}

object OrdersRevenue extends App {
  DatasetUtils.toDataFrame(DatasetUtils.orderItems)
    .join(DatasetUtils.toDataFrame(DatasetUtils.products), Seq("product_id"), "inner")
    .withColumn("revenue", col("price") * col("quantity"))
    .groupBy(col("order_id"))
    .agg(sum("revenue").as("total_revenue"), count("*").as("total_orders"))
    .orderBy(desc("total_revenue"))
    .show(false)
}
