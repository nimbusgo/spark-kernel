package com.ibm.spark.kernel.interpreter.pyspark

import java.util.concurrent.ConcurrentHashMap

import PySparkTypes._
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, promise}

/**
 * Represents the state structure of PySpark.
 *
 * @param maxQueuedCode The maximum amount of code to support being queued
 *                      at the same time for PySpark execution
 */
class PySparkState(private val maxQueuedCode: Int) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  import scala.collection.JavaConverters._

  private var _isReady: Boolean = false
  private val codeQueue: java.util.Queue[PySparkCode] =
    new java.util.concurrent.ConcurrentLinkedQueue[PySparkCode]()
  private val promiseMap: collection.mutable.Map[CodeId, PySparkPromise] =
    new ConcurrentHashMap[CodeId, PySparkPromise]().asScala

  /**
   * Adds new code to eventually be executed by Python.
   *
   * @param code The snippet of code to execute
   *
   * @return The future containing the results of the execution
   */
  def pushCode(code: Code): Future[CodeResults] = synchronized {
    // Throw the standard error if our maximum limit has been reached
    if (codeQueue.size() >= maxQueuedCode)
      throw new IllegalStateException(
        s"Code limit of $maxQueuedCode has been reached!")

    // Generate our promise that will be fulfilled when the code is executed
    // and the results are sent back
    val codeExecutionPromise = promise[CodeResults]()

    // Build the code representation to send to PySpark
    val uniqueId = java.util.UUID.randomUUID().toString
    val pySparkCode = PySparkCode(uniqueId, code)
    val pySparkPromise = PySparkPromise(uniqueId, codeExecutionPromise)

    logger.debug(s"Queueing '$code' with id '$uniqueId' to run with PySpark")

    // Add the code to be executed to our queue and the promise to our map
    codeQueue.add(pySparkCode)
    promiseMap.put(pySparkPromise.codeId, pySparkPromise)

    codeExecutionPromise.future
  }

  /**
   * Returns the total code currently queued to be executed.
   *
   * @return The total number of code instances queued to be executed
   */
  def totalQueuedCode(): Int = codeQueue.size()

  /**
   * Retrieves (and removes) the next piece of code to be executed.
   *
   * @note This should only be invoked by the Python process!
   *
   * @return The next code to execute if available, otherwise null
   */
  def nextCode(): PySparkCode = {
    val pySparkCode = codeQueue.poll()

    if (pySparkCode != null)
      logger.trace(s"Sending $pySparkCode to PySpark runner")

    pySparkCode
  }

  /**
   * Indicates whether or not the PySpark instance is ready for code.
   *
   * @return True if it is ready, otherwise false
   */
  def isReady: Boolean = _isReady

  /**
   * Marks the state of PySpark as ready.
   */
  def markReady(): Unit = _isReady = true

  /**
   * Marks the specified code as successfully completed using its id.
   *
   * @param codeId The id of the code to mark as a failure
   * @param output The output from the execution to be used as the result
   */
  def markSuccess(codeId: CodeId, output: CodeResults): Unit = {
    logger.debug(s"Received success for code with id '$codeId': $output")
    promiseMap.remove(codeId).foreach(_.promise.success(output))
  }

  /**
   * Marks the specified code as unsuccessful using its id.
   *
   * @param codeId The id of the code to mark as a failure
   * @param output The output from the error to be used as the description
   *               of the exception
   */
  def markFailure(codeId: CodeId, output: CodeResults): Unit = {
    logger.debug(s"Received failure for code with id '$codeId': $output")
    promiseMap.remove(codeId).foreach(
      _.promise.failure(new PySparkException(output)))
  }
}

