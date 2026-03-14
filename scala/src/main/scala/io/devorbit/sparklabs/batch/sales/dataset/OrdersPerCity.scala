package io.devorbit.sparklabs.batch.sales.dataset

import org.apache.spark.sql.functions.{col, count, desc}

object OrdersPerCity extends App {
  DatasetUtils.toDataFrame(DatasetUtils.orders)
    .join(DatasetUtils.toDataFrame(DatasetUtils.customers), Seq("customer_id"), "inner")
    .groupBy(col("city"))
    .agg(count("*").as("total_orders_per_city"))
    .orderBy(desc("total_orders_per_city"))
    .show(false)
}
