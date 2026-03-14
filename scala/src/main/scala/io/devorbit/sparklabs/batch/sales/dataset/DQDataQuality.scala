package io.devorbit.sparklabs.batch.sales.dataset

import io.devorbit.sparklabs.batch.sales.dataframe.Utils

object DQDataQuality extends App {
  import Utils.sparkSession.implicits._

  val duplicates = DatasetUtils.customers
    .groupByKey(_.customer_id)
    .count()
    .filter(_._2 > 1)

  if (duplicates.count() == 0) println("PASS: customer_id is unique")
  else duplicates.show(false)
}
