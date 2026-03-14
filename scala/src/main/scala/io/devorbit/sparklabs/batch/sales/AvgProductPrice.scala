package io.devorbit.sparklabs.batch.sales

import org.apache.spark.sql.functions._

object AvgProductPrice extends App {
  Utils.products
    .groupBy(col("category"))
    .agg(round(avg("price")).as("avg_price"))
    .orderBy(desc("avg_price"))
    .show(false)

//  Thread.sleep(100000000)

}
