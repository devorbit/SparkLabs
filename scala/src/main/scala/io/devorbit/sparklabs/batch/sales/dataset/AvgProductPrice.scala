package io.devorbit.sparklabs.batch.sales.dataset

import org.apache.spark.sql.functions.{avg, col, desc, round}

object AvgProductPrice extends App {
  DatasetUtils.toDataFrame(DatasetUtils.products)
    .groupBy(col("category"))
    .agg(round(avg("price")).as("avg_price"))
    .orderBy(desc("avg_price"))
    .show(false)
}
