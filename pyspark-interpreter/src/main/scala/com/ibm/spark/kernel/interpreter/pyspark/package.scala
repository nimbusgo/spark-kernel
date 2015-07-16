package com.ibm.spark.kernel.interpreter

import com.ibm.spark.interpreter.broker.{BrokerCode, BrokerPromise}

/**
 * Contains aliases to broker types.
 */
package object pyspark {
  /**
   * Represents a promise made regarding the completion of PySpark code
   * execution.
   */
  type PySparkPromise = BrokerPromise

  /**
   * Represents a block of PyPython code to be evaluated.
   */
  type PySparkCode = BrokerCode
}
