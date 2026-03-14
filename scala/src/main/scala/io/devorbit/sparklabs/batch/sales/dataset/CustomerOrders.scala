package io.devorbit.sparklabs.batch.sales.dataset

import org.apache.spark.sql.functions.{approx_count_distinct, col, desc}

object CustomerOrders extends App {
  DatasetUtils.toDataFrame(DatasetUtils.customers)
    .join(DatasetUtils.toDataFrame(DatasetUtils.orders), Seq("customer_id"), "inner")
    .groupBy(col("customer_id"))
    .agg(approx_count_distinct("customer_id").as("total_orders"))
    .orderBy(desc("total_orders"))
    .show(false)
}
