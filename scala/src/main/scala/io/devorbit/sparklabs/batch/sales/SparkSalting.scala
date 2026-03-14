package io.devorbit.sparklabs.batch.sales

import io.devorbit.sparklabs.batch.sales.Utils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SparkSalting extends App {

  val df2 =
    Utils.sparkSession.range(20)
      .withColumn("salt", (rand() * 10).cast("int"))
      .show(false)

//  Thread.sleep(1000000)
}
