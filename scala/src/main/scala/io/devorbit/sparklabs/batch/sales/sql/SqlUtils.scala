package io.devorbit.sparklabs.batch.sales.sql

import io.devorbit.sparklabs.batch.sales.dataframe.Utils
import org.apache.spark.sql.DataFrame

object SqlUtils {

  private def ensureViews(): Unit = Utils.registerTempViews()

  def sql(query: String): DataFrame = {
    ensureViews()
    Utils.sparkSession.sql(query)
  }
}
