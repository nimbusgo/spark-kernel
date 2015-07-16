package com.ibm.spark.kernel.interpreter.pyspark

import com.ibm.spark.interpreter.broker.BrokerBridge
import com.ibm.spark.kernel.api.KernelLike
import org.apache.spark.SparkContext

/**
 * Represents constants for the PySpark bridge.
 */
object PySparkBridge {
  /** Represents the maximum amount of code that can be queued for Python. */
  val MaxQueuedCode = 500
}

/**
 * Represents the API available to PySpark to act as the bridge for data
 * between the JVM and Python.
 *
 * @param _kernel The kernel API to expose through the bridge
 * @param _sparkContext The SparkContext to expose through the bridge
 */
class PySparkBridge(
  private val _kernel: KernelLike,
  private val _sparkContext: SparkContext
) extends BrokerBridge(_kernel, _sparkContext) {
  override val brokerName: String = "PySpark"
}
