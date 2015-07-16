package com.ibm.spark.kernel.interpreter.r

import com.ibm.spark.kernel.interpreter.r.SparkRTypes.{CodeResults, CodeId}

import scala.concurrent.Promise

/**
 * Represents a promise made regarding the completion of SparkR code execution.
 *
 * @param codeId The id of the code that was executed
 * @param promise The promise to be fulfilled when the code finishes executing
 */
case class SparkRPromise(codeId: CodeId, promise: Promise[CodeResults])
