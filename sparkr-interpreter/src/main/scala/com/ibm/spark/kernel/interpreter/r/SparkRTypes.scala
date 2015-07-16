package com.ibm.spark.kernel.interpreter.r

/**
 * Represents all types associated with the SparkR interface.
 */
object SparkRTypes {
  /** Represents the id used to keep track of executing code. */
  type CodeId = String

  /** Represents the code to execute on Python. */
  type Code = String

  /** Represents the results of code execution or the failure message. */
  type CodeResults = String
}
