package com.ibm.spark.kernel.interpreter.pyspark

/**
 * Represents all types associated with the PySpark interface.
 */
object PySparkTypes {
  /** Represents the id used to keep track of executing code. */
  type CodeId = String

  /** Represents the code to execute on Python. */
  type Code = String

  /** Represents the results of code execution or the failure message. */
  type CodeResults = String
}
