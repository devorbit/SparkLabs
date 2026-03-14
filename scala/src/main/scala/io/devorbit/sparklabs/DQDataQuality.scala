package io.devorbit.sparklabs

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}

object DQDataQuality extends App {

  val customers = Utils.customers
  val verificationResult = VerificationSuite()
    .onData(customers)
    .addCheck(
      Check(CheckLevel.Error, "Customer should not have duplicate IDs")
        .isUnique("customer_id")
    ).run()

  verificationResult.checkResults.foreach(println(_))
  Thread.sleep(1000000)

}
