package com.ibm.spark.interpreter.broker

import com.ibm.spark.interpreter.broker.BrokerTypes.{Code, CodeResults}
import scala.concurrent.Future

/**
 * Represents the service that provides the high-level interface between the
 * JVM and another process.
 */
trait BrokerService {
  /** Starts the broker service. */
  def start(): Unit

  /**
   * Indicates whether or not the service is running.
   *
   * @return True if running, otherwise false
   */
  def isRunning: Boolean

  /**
   * Submits code to the broker service to be executed and return a result.
   *
   * @param code The code to execute
   *
   * @return The result as a future to eventually return
   */
  def submitCode(code: Code): Future[CodeResults]

  /** Stops the running broker service. */
  def stop(): Unit
}
