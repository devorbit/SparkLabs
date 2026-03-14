package io.devorbit.sparklabs.batch.sales.dataframe

import org.apache.spark.sql.functions._

object OrdersRevenueCaseWhen extends App {

  Utils.orderItems
    .join(Utils.orders, Seq("order_id"), "inner")
    .join(Utils.products, Seq("product_id"), "inner")
    .join(Utils.promotions, Seq("category"), "left")
    .withColumn("gross_revenue", col("price") * col("quantity"))
    .withColumn(
      "discount_pct",
      when(
        to_date(col("order_date")).between(to_date(col("start_date")), to_date(col("end_date"))),
        coalesce(col("discount_pct"), lit(0))
      ).otherwise(lit(0))
    )
    .withColumn("discount_value", col("gross_revenue") * col("discount_pct") / 100)
    .withColumn("net_revenue", col("gross_revenue") - col("discount_value"))
    .groupBy(col("order_id"))
    .agg(
      round(sum("gross_revenue"), 2).as("gross_revenue"),
      round(sum("discount_value"), 2).as("discount_value"),
      round(sum("net_revenue"), 2).as("net_revenue")
    )
    .orderBy(desc("net_revenue"))
    .show(false)

//  Thread.sleep(1000000)
}
