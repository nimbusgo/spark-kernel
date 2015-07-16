package com.ibm.spark.kernel.interpreter.r

import com.ibm.spark.interpreter.broker.BrokerState

/**
 * Represents the state structure of SparkR.
 *
 * @param maxQueuedCode The maximum amount of code to support being queued
 *                      at the same time for SparkR execution
 */
class SparkRState(private val maxQueuedCode: Int)
  extends BrokerState(maxQueuedCode)
