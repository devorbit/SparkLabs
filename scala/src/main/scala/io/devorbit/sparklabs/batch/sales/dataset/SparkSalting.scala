package io.devorbit.sparklabs.batch.sales.dataset

import io.devorbit.sparklabs.batch.sales.dataframe.Utils

object SparkSalting extends App {
  import Utils.sparkSession.implicits._

  Utils.sparkSession.range(20).as[Long]
    .map(id => (id, (scala.util.Random.nextDouble() * 10).toInt))
    .toDF("id", "salt")
    .show(false)
}
