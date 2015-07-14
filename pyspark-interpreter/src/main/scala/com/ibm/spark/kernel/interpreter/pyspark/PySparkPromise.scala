package com.ibm.spark.kernel.interpreter.pyspark

import com.ibm.spark.kernel.interpreter.pyspark.PySparkTypes._

import scala.concurrent.Promise

/**
 * Represents a promise made regarding the completion of PySpark code execution.
 *
 * @param codeId The id of the code that was executed
 * @param promise The promise to be fulfilled when the code finishes executing
 */
case class PySparkPromise(codeId: CodeId, promise: Promise[CodeResults])
