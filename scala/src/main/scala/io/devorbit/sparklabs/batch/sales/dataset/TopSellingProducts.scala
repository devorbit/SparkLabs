package io.devorbit.sparklabs.batch.sales.dataset

import org.apache.spark.sql.functions.{col, count, desc}

object TopSellingProducts extends App {
  DatasetUtils.toDataFrame(DatasetUtils.orderItems)
    .join(DatasetUtils.toDataFrame(DatasetUtils.products), Seq("product_id"), "inner")
    .groupBy(col("product_id"))
    .agg(count("*").as("total_orders"))
    .orderBy(desc("total_orders"))
    .show(false)
}
