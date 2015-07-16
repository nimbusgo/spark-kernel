package com.ibm.spark.kernel.interpreter.pyspark

import com.ibm.spark.interpreter.broker.BrokerProcessHandler

/**
 * Represents the handler for events triggered by the PySpark process.
 *
 * @param pySparkBridge The bridge to reset when the process fails or completes
 * @param restartOnFailure If true, restarts the process if it fails
 * @param restartOnCompletion If true, restarts the process if it completes
 */
class PySparkProcessHandler(
  private val pySparkBridge: PySparkBridge,
  private val restartOnFailure: Boolean,
  private val restartOnCompletion: Boolean
  ) extends BrokerProcessHandler(
  pySparkBridge,
  restartOnFailure,
  restartOnCompletion
) {
  override val brokerName: String = "PySpark"
}

