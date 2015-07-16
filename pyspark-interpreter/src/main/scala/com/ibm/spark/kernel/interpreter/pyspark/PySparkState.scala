package com.ibm.spark.kernel.interpreter.pyspark

import com.ibm.spark.interpreter.broker.BrokerState

/**
 * Represents the state structure of PySpark.
 *
 * @param maxQueuedCode The maximum amount of code to support being queued
 *                      at the same time for PySpark execution
 */
class PySparkState(private val maxQueuedCode: Int)
  extends BrokerState(maxQueuedCode)