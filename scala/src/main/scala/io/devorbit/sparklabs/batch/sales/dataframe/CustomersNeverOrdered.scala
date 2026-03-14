package io.devorbit.sparklabs.batch.sales.dataframe

import org.apache.spark.sql.functions._

object CustomersNeverOrdered extends App {
  Utils.customers
    .join(Utils.orders, Seq("customer_id"), "left_anti")
    .show(false)
//  Thread.sleep(100000000)

}
