package com.ibm.spark.interpreter.broker

/**
 * Represents all types associated with the broker interface.
 */
object BrokerTypes {
  /** Represents the id used to keep track of executing code. */
  type CodeId = String

  /** Represents the code to execute. */
  type Code = String

  /** Represents the results of code execution or the failure message. */
  type CodeResults = String
}
