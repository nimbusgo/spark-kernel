package com.ibm.spark.kernel.interpreter.r

import com.ibm.spark.interpreter.broker.BrokerProcessHandler

/**
 * Represents the handler for events triggered by the SparkR process.
 *
 * @param sparkRBridge The bridge to reset when the process fails or completes
 * @param restartOnFailure If true, restarts the process if it fails
 * @param restartOnCompletion If true, restarts the process if it completes
 */
class SparkRProcessHandler(
  private val sparkRBridge: SparkRBridge,
  private val restartOnFailure: Boolean,
  private val restartOnCompletion: Boolean
) extends BrokerProcessHandler(
  sparkRBridge,
  restartOnFailure,
  restartOnCompletion
) {
  override val brokerName: String = "SparkR"
}
