package com.ibm.spark.interpreter.broker

/**
 * Provides broker types to the class/trait that implements this trait.
 */
trait BrokerTypesProvider {
  /** Represents the id used to keep track of executing code. */
  type CodeId = String

  /** Represents the code to execute. */
  type Code = String

  /** Represents the results of code execution or the failure message. */
  type CodeResults = String
}
