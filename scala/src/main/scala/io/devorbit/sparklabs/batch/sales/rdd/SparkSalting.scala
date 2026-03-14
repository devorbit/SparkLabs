package io.devorbit.sparklabs.batch.sales.rdd

object SparkSalting extends App {
  RddUtils.orders.context.parallelize(0 until 20)
    .map(id => (id, (scala.util.Random.nextDouble() * 10).toInt))
    .collect()
    .foreach(println)
}
