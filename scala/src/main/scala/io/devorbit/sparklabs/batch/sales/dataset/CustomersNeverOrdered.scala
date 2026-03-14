package io.devorbit.sparklabs.batch.sales.dataset

object CustomersNeverOrdered extends App {
  DatasetUtils.toDataFrame(DatasetUtils.customers)
    .join(DatasetUtils.toDataFrame(DatasetUtils.orders), Seq("customer_id"), "left_anti")
    .show(false)
}
