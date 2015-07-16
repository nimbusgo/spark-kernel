package com.ibm.spark.kernel.interpreter

import com.ibm.spark.interpreter.broker.{BrokerCode, BrokerPromise}

/**
 * Contains aliases to broker types.
 */
package object r {
  /**
   * Represents a promise made regarding the completion of SparkR code
   * execution.
   */
  type SparkRPromise = BrokerPromise

  /**
   * Represents a block of SparkR code to be evaluated.
   */
  type SparkRCode = BrokerCode
}
