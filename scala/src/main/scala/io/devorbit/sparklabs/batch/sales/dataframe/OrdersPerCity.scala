package io.devorbit.sparklabs.batch.sales.dataframe

import org.apache.spark.sql.functions._

object OrdersPerCity extends App {

  Utils.orders.join(Utils.customers, Seq("customer_id"), "inner")
    .groupBy(col("city"))
    .agg(count("*").as("total_orders_per_city"))
    .orderBy(desc("total_orders_per_city"))
    .show(false)

//  Thread.sleep(1000000)
}
