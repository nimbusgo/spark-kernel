package com.ibm.spark.kernel.interpreter.pyspark

import PySparkTypes._

/**
 * Represents a block of Python code to be evaluated.
 *
 * @param codeId The id to associate with the code to be executed
 * @param code The code to evaluate using PySpark
 */
case class PySparkCode(codeId: CodeId, code: Code)

