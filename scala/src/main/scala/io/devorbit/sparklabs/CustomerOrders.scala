package io.devorbit.sparklabs

import org.apache.spark.sql.functions._

object CustomerOrders extends App {

  Utils.customers.join(Utils.orders, Seq("customer_id"), "inner")
    .groupBy(col("customer_id"))
    .agg(approx_count_distinct("customer_id").as("total_orders"))
    .orderBy(desc("total_orders"))
    .show(false)

  Thread.sleep(1000000)
}
